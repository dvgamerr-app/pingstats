/*
 * Copyright (c) 2016 - 2017 cooky451
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

#pragma once

#include "utility/utility.hpp"
#include "utility/scoped_thread.hpp"
#include "utility/tree_config.hpp"
#include "winapi/utility.hpp"
#include "icmp.hpp"

#include <libpq-fe.h>

#include <algorithm>
#include <condition_variable>
#include <deque>
#include <iterator>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace pingstats // export
{
	using namespace utility::literals;

	namespace cr = std::chrono;
	namespace ut = utility;
	namespace wa = winapi;

	// Optional second destination for ping results: everything that goes
	// into the SQLite log is also queued for a PostgreSQL server and sent
	// there once a minute.
	//
	// The insert never happens on the caller's thread. Results arrive on the
	// UI thread (the ping threads hand them over with SendMessage), and a
	// round trip to a database over the network there would stall both the
	// drawing and the pings. Instead results go into a bounded queue that a
	// worker thread drains on its own, reconnecting and retrying when the
	// server is unreachable.
	//
	// Everything lands in one table. That table is range partitioned by
	// week, and the weekly partition is created on demand, so a year of
	// results stays one thing to query and an old week is one DROP TABLE
	// to get rid of.
	class PostgresLog
	{
		struct Row
		{
			std::int64_t unixTimeMs;
			std::string section;
			std::optional<double> latencyMs; // empty when the ping was lost
			std::uint32_t errorCode;
			std::uint32_t statusCode;
			std::string responder;
			std::uint32_t sysLatencyMs;
		};

		static constexpr std::size_t COLUMNS_PER_ROW{ 8 };
		static constexpr std::int64_t MS_PER_DAY{ 24 * 3600 * 1000 };

		std::string _uri;
		std::string _table{ "pingstats" };
		std::string _hostname{ computerName() };

		std::uint32_t _syncSeconds{ 60 };

		// Counted in days like the SQLite log's retentionDays, but enforced
		// a whole week at a time: a weekly partition is only dropped once
		// every result in it has aged out. 0 keeps everything.
		std::uint32_t _retentionDays{ 30 };

		// Rows per INSERT. Only reached after an outage, when a backlog is
		// flushed; a normal minute is a couple hundred rows in total.
		std::size_t _batchSize{ 500 };
		std::size_t _queueLimit{ 100000 };

		PGconn* _connection{};
		std::string _quotedTable;
		bool _partitioned{};
		std::int64_t _currentWeekStartMs{ -1 };
		bool _reportedFailure{};

		std::mutex _mutex;
		std::condition_variable _condition;
		std::deque<Row> _queue;
		bool _stop{};

		// Declared last: the worker uses every member above it, so it has
		// to be joined before any of them are destroyed.
		ut::AutojoinThread _thread;

	public:
		~PostgresLog()
		{
			{
				std::lock_guard<std::mutex> lock{ _mutex };
				_stop = true;
			}

			_condition.notify_all();
		}

		PostgresLog(PostgresLog&&) = delete;

		// Config (in the same top-level db node as the SQLite log):
		//
		// db {
		//     postgresUri = ;               // empty = no PostgreSQL sync
		//     postgresTable = pingstats;
		//     postgresSyncSeconds = 60;
		//     postgresRetentionDays = 30;
		// }
		//
		// postgresUri is a libpq connection URI, e.g.
		// postgresql://user:password@host:5432/dbname?sslmode=require
		PostgresLog(ut::TreeConfigNode& config)
		{
			auto& dbcfg{ *config.findOrAppendNode("db") };

			dbcfg.loadOrStore("postgresUri", _uri);
			dbcfg.loadOrStore("postgresTable", _table);
			dbcfg.loadOrStore("postgresSyncSeconds", _syncSeconds);
			dbcfg.loadOrStore("postgresRetentionDays", _retentionDays);

			_syncSeconds = std::max(1u, _syncSeconds);

			if (!enabled())
			{
				return;
			}

			_thread = std::thread([this] { run(); });
		}

		bool enabled() const
		{
			return _uri.size() > 0 && _table.size() > 0;
		}

		void log(const std::string& section, const IcmpEchoResult& result)
		{
			if (!enabled())
			{
				return;
			}

			const auto isLost{ result.errorCode != 0 || result.statusCode != 0 };

			Row row;

			row.unixTimeMs = toUnixTimeMs(result.sentTime);
			row.section = section;
			row.errorCode = result.errorCode;
			row.statusCode = result.statusCode;
			row.responder = result.responder.name();
			row.sysLatencyMs = result.sysLatency;

			if (!isLost)
			{
				row.latencyMs = ut::milliseconds_f64(result.latency).count();
			}

			std::lock_guard<std::mutex> lock{ _mutex };

			_queue.push_back(std::move(row));
			trimQueue();
		}

	private:
		static std::string computerName()
		{
			wchar_t buffer[MAX_COMPUTERNAME_LENGTH + 1]{};
			DWORD size{ MAX_COMPUTERNAME_LENGTH + 1 };

			if (!GetComputerNameW(buffer, &size))
			{
				return "unknown";
			}

			return wa::utf8(std::wstring_view{ buffer, size });
		}

		// Called with _mutex held: drops the oldest results once the server
		// has been unreachable long enough to fill the queue. Losing old
		// rows is better than growing until the process runs out of memory,
		// and SQLite still has all of them.
		void trimQueue()
		{
			while (_queue.size() > _queueLimit)
			{
				_queue.pop_front();
			}
		}

		void run()
		{
			// The first flush comes early rather than a full interval in:
			// it gets the connection up (so a short session still syncs,
			// and shutdown has somewhere to send the tail), and a wrong URI
			// is reported within seconds instead of within a minute.
			auto interval{ cr::seconds{ 5 } };

			for (;;)
			{
				bool stopping;

				{
					std::unique_lock<std::mutex> lock{ _mutex };

					_condition.wait_for(lock, interval, [this] { return _stop; });

					stopping = _stop;
				}

				interval = cr::seconds{ _syncSeconds };

				flushQueue(stopping);

				if (stopping)
				{
					break;
				}
			}

			disconnect();
		}

		// Sends whatever has piled up since the last tick, in batches.
		// Returns as soon as something fails; the next tick tries again
		// with the failed rows back at the front of the queue.
		void flushQueue(bool stopping)
		{
			for (;;)
			{
				std::vector<Row> batch;

				{
					std::lock_guard<std::mutex> lock{ _mutex };

					const auto count{ std::min(_batchSize, _queue.size()) };

					// Nothing to send, or shutting down without a live
					// connection - dialling out now would only make closing
					// the window take longer, and SQLite has the rows.
					if (count == 0 || (stopping && !isConnected()))
					{
						return;
					}

					batch.assign(
						std::make_move_iterator(_queue.begin()),
						std::make_move_iterator(_queue.begin() + count));

					_queue.erase(_queue.begin(), _queue.begin() + count);
				}

				if (!insertBatch(batch))
				{
					std::lock_guard<std::mutex> lock{ _mutex };

					// Back in front of whatever arrived while we were
					// trying, so results keep their order.
					_queue.insert(_queue.begin(),
						std::make_move_iterator(batch.begin()),
						std::make_move_iterator(batch.end()));

					trimQueue();

					return;
				}
			}
		}

		bool isConnected() const
		{
			return _connection != nullptr &&
				PQstatus(_connection) == CONNECTION_OK;
		}

		void disconnect()
		{
			if (_connection != nullptr)
			{
				PQfinish(_connection);
				_connection = nullptr;
			}
		}

		bool connect()
		{
			if (isConnected())
			{
				return true;
			}

			disconnect();

			// libpq expands a connection URI passed as "dbname" (that is
			// what the trailing 1 asks for), which lets us keep the
			// single-URI config and still supply defaults. Ours come first
			// so anything the URI itself sets wins over them.
			const char* keys[]{
				"connect_timeout", "application_name", "dbname", nullptr };

			const char* values[]{
				"3", "pingstats", _uri.c_str(), nullptr };

			_connection = PQconnectdbParams(keys, values, 1);

			if (!isConnected())
			{
				reportFailure("Connection failed: "s + PQerrorMessage(_connection));
				disconnect();

				return false;
			}

			if (!prepareSchema())
			{
				disconnect();

				return false;
			}

			_reportedFailure = false;

			return true;
		}

		// Runs once per connection: look the table up, and create it if it
		// isn't there yet.
		bool prepareSchema()
		{
			const auto quoted{ quoteIdentifier(_table) };

			if (!quoted.has_value())
			{
				reportFailure("Invalid table name: "s + _table);

				return false;
			}

			_quotedTable = *quoted;

			std::string error;

			if (!exec("SET statement_timeout = 10000;", error))
			{
				reportFailure("Statement failed: " + error);

				return false;
			}

			// The quoted name, so a table whose name isn't all lower case
			// is looked up as the same identifier we create it with.
			const auto kind{ relationKind(_quotedTable) };

			if (!kind.has_value())
			{
				return false;
			}

			if (kind->size() == 0)
			{
				return createTable();
			}

			// 'p' is a partitioned table, anything else is a plain one that
			// somebody else set up. Then the weekly partitions aren't ours
			// to manage and rows just go straight into it.
			_partitioned = (*kind == "p");

			pruneOldPartitions();

			return true;
		}

		// Empty string means the relation doesn't exist. No value means the
		// lookup itself failed.
		std::optional<std::string> relationKind(const std::string& table)
		{
			const char* params[]{ table.c_str() };

			const auto result{ PQexecParams(_connection,
				"SELECT relkind FROM pg_class WHERE oid = to_regclass($1);",
				1, nullptr, params, nullptr, nullptr, 0) };

			if (result == nullptr || PQresultStatus(result) != PGRES_TUPLES_OK)
			{
				reportFailure("Table lookup failed: "s + PQerrorMessage(_connection));

				if (result != nullptr)
				{
					PQclear(result);
				}

				return std::nullopt;
			}

			std::string kind;

			if (PQntuples(result) > 0 && !PQgetisnull(result, 0, 0))
			{
				kind = PQgetvalue(result, 0, 0);
			}

			PQclear(result);

			return kind;
		}

		bool createTable()
		{
			const auto index{ quoteIdentifier(_table + "_ts_idx") };

			if (!index.has_value())
			{
				return false;
			}

			// The weekly partitions themselves are created on demand, when
			// the first result of a week is inserted.
			const auto sql{
				"CREATE TABLE " + _quotedTable + " ("
				"ts TIMESTAMPTZ NOT NULL,"
				"host TEXT NOT NULL,"
				"section TEXT NOT NULL,"
				"latency_ms DOUBLE PRECISION," // NULL when the ping was lost
				"error_code INTEGER NOT NULL,"
				"status_code INTEGER NOT NULL,"
				"responder TEXT,"
				"sys_latency_ms INTEGER) PARTITION BY RANGE (ts);"

				// On a partitioned parent this is created on every
				// partition, including the ones added later.
				"CREATE INDEX " + *index + " ON " + _quotedTable +
				" (ts, host, section);" };

			std::string error;

			if (!exec(sql, error))
			{
				reportFailure("Creating table " + _table + " failed: " + error);

				return false;
			}

			_partitioned = true;

			return true;
		}

		// Weeks start on Monday 00:00 UTC and are named after the ISO year
		// and week they hold, e.g. pingstats_2026w38. UTC keeps the
		// boundaries the same for every machine writing to the table.
		static std::int64_t weekStart(std::int64_t unixTimeMs)
		{
			const auto days{ unixTimeMs / MS_PER_DAY };

			// 1970-01-01 was a Thursday, three days into its week.
			return (days - (days + 3) % 7) * MS_PER_DAY;
		}

		static std::string partitionName(
			const std::string& table, std::int64_t weekStartMs)
		{
			// The ISO week is the one holding that week's Thursday.
			const auto thursday{ static_cast<std::time_t>(
				(weekStartMs + 3 * MS_PER_DAY) / 1000) };

			std::tm tm;
			gmtime_s(&tm, &thursday);

			return table + ut::formatString("_%04dw%02d",
				1900 + tm.tm_year, tm.tm_yday / 7 + 1);
		}

		static std::string timestampLiteral(std::int64_t unixTimeMs)
		{
			const auto stamp{ static_cast<std::time_t>(unixTimeMs / 1000) };

			std::tm tm;
			gmtime_s(&tm, &stamp);

			return ut::formatString("%04d-%02d-%02d %02d:%02d:%02d+00",
				1900 + tm.tm_year, 1 + tm.tm_mon, tm.tm_mday,
				tm.tm_hour, tm.tm_min, tm.tm_sec);
		}

		bool ensurePartition(std::int64_t unixTimeMs)
		{
			const auto start{ weekStart(unixTimeMs) };

			if (!_partitioned || start == _currentWeekStartMs)
			{
				return true;
			}

			const auto name{ quoteIdentifier(partitionName(_table, start)) };

			if (!name.has_value())
			{
				return false;
			}

			const auto sql{
				"CREATE TABLE IF NOT EXISTS " + *name +
				" PARTITION OF " + _quotedTable + " FOR VALUES FROM ('" +
				timestampLiteral(start) + "') TO ('" +
				timestampLiteral(start + 7 * MS_PER_DAY) + "');" };

			std::string error;

			if (!exec(sql, error))
			{
				reportFailure("Creating weekly partition failed: " + error);

				return false;
			}

			_currentWeekStartMs = start;

			// A new week is exactly when an old one falls out of the
			// retention window, and the rest of the time this is one cheap
			// query that finds nothing to do.
			pruneOldPartitions();

			return true;
		}

		// Drops whole weekly partitions that fell out of the retention
		// window. Only partitions of our own table, and only ones named the
		// way we name them, are ever touched - a partition somebody added
		// by hand is left alone.
		void pruneOldPartitions()
		{
			if (!_partitioned || _retentionDays == 0)
			{
				return;
			}

			const auto cutoff{ toUnixTimeMs(cr::steady_clock::now()) -
				std::int64_t{ _retentionDays } * MS_PER_DAY };

			// A partition holds the week [start, start + 7 days), so it can
			// only go once its last result is past the cutoff. Results
			// therefore live between retentionDays and retentionDays plus a
			// week - never less than asked for.
			const auto lastExpired{ cutoff - 7 * MS_PER_DAY };

			if (lastExpired < 0)
			{
				return;
			}

			const auto lastExpiredName{
				partitionName(_table, weekStart(lastExpired)) };

			const char* params[]{ _quotedTable.c_str() };

			// oid::regclass::text comes back schema qualified and quoted
			// exactly as DROP TABLE needs it.
			const auto result{ PQexecParams(_connection,
				"SELECT c.relname, c.oid::regclass::text FROM pg_inherits i "
				"JOIN pg_class c ON c.oid = i.inhrelid "
				"WHERE i.inhparent = to_regclass($1);",
				1, nullptr, params, nullptr, nullptr, 0) };

			if (result == nullptr || PQresultStatus(result) != PGRES_TUPLES_OK)
			{
				if (result != nullptr)
				{
					PQclear(result);
				}

				return;
			}

			std::vector<std::string> toDrop;

			for (auto i{ 0 }; i < PQntuples(result); ++i)
			{
				const std::string name{ PQgetvalue(result, i, 0) };

				// The names carry ISO year then week, both zero padded, so
				// they sort in the same order as the weeks themselves.
				if (isWeeklyPartitionName(name) && name <= lastExpiredName)
				{
					toDrop.emplace_back(PQgetvalue(result, i, 1));
				}
			}

			PQclear(result);

			for (auto& partition : toDrop)
			{
				std::string error;

				if (!exec("DROP TABLE IF EXISTS " + partition + ";", error))
				{
					reportFailure("Dropping expired partition " +
						partition + " failed: " + error);

					return;
				}
			}
		}

		// <table>_YYYYwWW, as written by partitionName().
		bool isWeeklyPartitionName(const std::string& name) const
		{
			const auto suffixSize{ std::string("_0000w00").size() };

			if (name.size() != _table.size() + suffixSize ||
				name.compare(0, _table.size(), _table) != 0)
			{
				return false;
			}

			const auto suffix{ name.substr(_table.size()) };

			if (suffix[0] != '_' || suffix[5] != 'w')
			{
				return false;
			}

			for (const auto i : { 1, 2, 3, 4, 6, 7 })
			{
				if (std::isdigit(static_cast<unsigned char>(suffix[i])) == 0)
				{
					return false;
				}
			}

			return true;
		}

		std::optional<std::string> quoteIdentifier(const std::string& name)
		{
			const auto quoted{ PQescapeIdentifier(
				_connection, name.c_str(), name.size()) };

			if (quoted == nullptr)
			{
				return std::nullopt;
			}

			const std::string result{ quoted };
			PQfreemem(quoted);

			return result;
		}

		bool exec(const std::string& sql, std::string& error)
		{
			const auto result{ PQexec(_connection, sql.c_str()) };

			const auto ok{ result != nullptr &&
				(PQresultStatus(result) == PGRES_COMMAND_OK ||
					PQresultStatus(result) == PGRES_TUPLES_OK) };

			if (!ok)
			{
				error = PQerrorMessage(_connection);
			}

			if (result != nullptr)
			{
				PQclear(result);
			}

			return ok;
		}

		bool insertBatch(const std::vector<Row>& rows)
		{
			if (rows.size() == 0)
			{
				return true;
			}

			// Two attempts: between two once-a-minute flushes the server can
			// close an idle connection, and libpq only finds out when the
			// insert fails. A dead socket is worth one silent retry.
			for (auto attempt{ 0 }; attempt < 2; ++attempt)
			{
				if (!connect())
				{
					return false;
				}

				std::string error;

				if (tryInsertBatch(rows, error))
				{
					return true;
				}

				if (isConnected())
				{
					reportFailure("Insert failed: " + error);

					return false;
				}

				disconnect();

				if (attempt > 0)
				{
					reportFailure("Insert failed: " + error);
				}
			}

			return false;
		}

		bool tryInsertBatch(const std::vector<Row>& rows, std::string& error)
		{
			for (auto& row : rows)
			{
				if (!ensurePartition(row.unixTimeMs))
				{
					error = "no partition for the result's week";

					return false;
				}
			}

			std::string sql{ "INSERT INTO " + _quotedTable +
				" (ts, host, section, latency_ms, error_code, status_code,"
				" responder, sys_latency_ms) VALUES " };

			std::vector<std::optional<std::string>> values;
			values.reserve(rows.size() * COLUMNS_PER_ROW);

			for (std::size_t i{}; i < rows.size(); ++i)
			{
				const auto& row{ rows[i] };
				const auto p{ i * COLUMNS_PER_ROW };

				if (i > 0)
				{
					sql += ',';
				}

				sql += ut::formatString(
					"(to_timestamp($%zu::bigint / 1000.0),"
					"$%zu,$%zu,$%zu,$%zu,$%zu,$%zu,$%zu)",
					p + 1, p + 2, p + 3, p + 4, p + 5, p + 6, p + 7, p + 8);

				values.emplace_back(std::to_string(row.unixTimeMs));
				values.emplace_back(_hostname);
				values.emplace_back(row.section);

				values.emplace_back(row.latencyMs.has_value() ?
					std::optional<std::string>{
						ut::formatString("%.6f", *row.latencyMs) } :
					std::nullopt);

				values.emplace_back(std::to_string(row.errorCode));
				values.emplace_back(std::to_string(row.statusCode));
				values.emplace_back(row.responder);
				values.emplace_back(std::to_string(row.sysLatencyMs));
			}

			std::vector<const char*> params;
			params.reserve(values.size());

			for (auto& value : values)
			{
				params.push_back(value.has_value() ? value->c_str() : nullptr);
			}

			const auto result{ PQexecParams(_connection, sql.c_str(),
				static_cast<int>(params.size()), nullptr,
				params.data(), nullptr, nullptr, 0) };

			const auto ok{ result != nullptr &&
				PQresultStatus(result) == PGRES_COMMAND_OK };

			if (!ok)
			{
				error = PQerrorMessage(_connection);
			}

			if (result != nullptr)
			{
				PQclear(result);
			}

			return ok;
		}

		// Only the first failure of an outage is shown, so a misconfigured
		// URI is noticed immediately while a server that stays down doesn't
		// bury the user in message boxes. The box gets its own detached
		// thread: waiting for someone to click OK here would stall the
		// queue and, worse, hold up shutdown until the box is dismissed.
		void reportFailure(const std::string& what)
		{
			if (_reportedFailure)
			{
				return;
			}

			_reportedFailure = true;

			const auto message{ what +
				"\r\n\r\nPing results are still written to the SQLite log, "
				"and syncing will keep retrying in the background." };

			std::thread{ [message] {
				wa::showMessageBox("PostgreSQL sync", message);
			} }.detach();
		}
	};
}
