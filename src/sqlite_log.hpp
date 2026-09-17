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
#include "utility/tree_config.hpp"
#include "icmp.hpp"

#include <sqlite3.h>

#include <algorithm>
#include <cstdio>
#include <ctime>
#include <stdexcept>
#include <string>
#include <vector>

namespace pingstats // export
{
	using namespace utility::literals;

	namespace cr = std::chrono;
	namespace ut = utility;

	// Continuous ping-result logging into SQLite.
	//
	// - WAL journal mode: the writer never blocks concurrent readers and
	//   readers never block the writer, so an external tool (or `sqlite3`
	//   CLI) can read the file live while pingstats keeps writing.
	// - Data is partitioned into one table per calendar day
	//   ("ping_log_YYYYMMDD"). Retention is enforced by DROP TABLE on whole
	//   expired days, which is effectively instant and avoids the
	//   DELETE + auto-vacuum cost of pruning rows out of one giant table.
	class SqliteLog
	{
		bool _enabled{ false };
		std::string _path{ "pingstats.db" };
		int _retentionDays{ 7 };
		int _restoreMinutes{ 60 };

		sqlite3* _db{};
		sqlite3_stmt* _insertStmt{};
		std::string _currentTable;

	public:
		~SqliteLog()
		{
			if (_insertStmt != nullptr)
			{
				sqlite3_finalize(_insertStmt);
			}

			if (_db != nullptr)
			{
				sqlite3_close(_db);
			}
		}

		SqliteLog(SqliteLog&&) = delete;

		// Config (top-level, shared by all sections):
		//
		// db {
		//     enabled = false;
		//     path = pingstats.db;
		//     retentionDays = 7;
		//     restoreMinutes = 60;
		// }
		SqliteLog(ut::TreeConfigNode& config)
		{
			auto& dbcfg{ *config.findOrAppendNode("db") };

			dbcfg.loadOrStore("enabled", _enabled);
			dbcfg.loadOrStore("path", _path);
			dbcfg.loadOrStore("retentionDays", _retentionDays);
			dbcfg.loadOrStore("restoreMinutes", _restoreMinutes);

			if (!_enabled)
			{
				return;
			}

			if (sqlite3_open_v2(_path.c_str(), &_db,
				SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
				nullptr) != SQLITE_OK)
			{
				const auto message{ "Failed to open sqlite database \""s +
					_path + "\": " + sqlite3_errmsg(_db) };

				sqlite3_close(_db);
				_db = nullptr;

				throw std::runtime_error(message);
			}

			exec("PRAGMA journal_mode=WAL;");
			exec("PRAGMA synchronous=NORMAL;");
			exec("PRAGMA busy_timeout=5000;");

			pruneOldPartitions();
		}

		bool enabled() const
		{
			return _enabled;
		}

		void log(const std::string& section, const IcmpEchoResult& result)
		{
			if (!_enabled)
			{
				return;
			}

			const auto stampMs{ toUnixTimeMs(result.sentTime) };
			const auto stamp{ static_cast<std::time_t>(stampMs / 1000) };

			std::tm tm;
			localtime_s(&tm, &stamp);

			ensureTableForDay(tm);

			const auto isLost{ result.errorCode != 0 || result.statusCode != 0 };

			sqlite3_reset(_insertStmt);
			sqlite3_clear_bindings(_insertStmt);

			sqlite3_bind_int64(_insertStmt, 1, static_cast<sqlite3_int64>(stampMs));
			sqlite3_bind_text(_insertStmt, 2, section.c_str(), -1, SQLITE_TRANSIENT);

			if (isLost)
			{
				sqlite3_bind_null(_insertStmt, 3);
			}
			else
			{
				sqlite3_bind_double(_insertStmt, 3,
					ut::milliseconds_f64(result.latency).count());
			}

			sqlite3_bind_int(_insertStmt, 4, static_cast<int>(result.errorCode));
			sqlite3_bind_int(_insertStmt, 5, static_cast<int>(result.statusCode));
			sqlite3_bind_text(_insertStmt, 6,
				result.responder.name().c_str(), -1, SQLITE_TRANSIENT);
			sqlite3_bind_int(_insertStmt, 7, static_cast<int>(result.sysLatency));

			if (sqlite3_step(_insertStmt) != SQLITE_DONE)
			{
				throw std::runtime_error(
					"Failed to insert ping result: "s + sqlite3_errmsg(_db));
			}
		}

		// Reads a section's recent history back out of the log, so a
		// restarted pingstats redraws the graph it had before instead of
		// starting from an empty plot. Rows come back oldest first, capped
		// at maxRows, and never older than the configured restore window -
		// after a long shutdown that window is simply empty.
		std::vector<IcmpEchoResult> loadRecent(
			const std::string& section, std::size_t maxRows)
		{
			std::vector<IcmpEchoResult> results;

			if (!_enabled || _restoreMinutes <= 0 || maxRows == 0)
			{
				return results;
			}

			const auto anchor{ cr::steady_clock::now() };
			const auto nowMs{ toUnixTimeMs(anchor) };
			const auto cutoffMs{ nowMs - std::int64_t{ _restoreMinutes } * 60 * 1000 };

			const auto tables{ partitionTablesSince(cutoffMs) };

			if (tables.empty())
			{
				return results;
			}

			// One numbered-parameter set shared by every partition branch.
			std::string sql;

			for (auto& table : tables)
			{
				if (!sql.empty())
				{
					sql += " UNION ALL ";
				}

				sql += "SELECT ts, latency_ms, error_code, status_code, responder, "
					"sys_latency_ms FROM \"" + table + "\" "
					"WHERE section = ?1 AND ts >= ?2";
			}

			sql += " ORDER BY ts DESC LIMIT ?3;";

			sqlite3_stmt* stmt{};

			if (sqlite3_prepare_v2(_db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK)
			{
				return results;
			}

			sqlite3_bind_text(stmt, 1, section.c_str(), -1, SQLITE_TRANSIENT);
			sqlite3_bind_int64(stmt, 2, cutoffMs);
			sqlite3_bind_int64(stmt, 3, static_cast<sqlite3_int64>(maxRows));

			while (sqlite3_step(stmt) == SQLITE_ROW)
			{
				IcmpEchoResult result{};

				result.sentTime = anchor -
					cr::milliseconds{ nowMs - sqlite3_column_int64(stmt, 0) };

				if (sqlite3_column_type(stmt, 1) != SQLITE_NULL)
				{
					result.latency = cr::duration_cast<cr::nanoseconds>(
						ut::milliseconds_f64{ sqlite3_column_double(stmt, 1) });
				}

				result.errorCode = static_cast<std::uint32_t>(
					sqlite3_column_int(stmt, 2));
				result.statusCode = static_cast<std::uint32_t>(
					sqlite3_column_int(stmt, 3));
				result.responder = IpEndPoint::fromAddressString(
					reinterpret_cast<const char*>(sqlite3_column_text(stmt, 4)));
				result.sysLatency = static_cast<std::uint32_t>(
					sqlite3_column_int(stmt, 5));

				results.push_back(result);
			}

			sqlite3_finalize(stmt);

			// The query takes the newest rows, the plot wants them in order.
			std::reverse(results.begin(), results.end());

			return results;
		}

	private:
		static std::string tableNameForDay(const std::tm& tm)
		{
			char buffer[32];

			std::snprintf(buffer, sizeof buffer, "ping_log_%04d%02d%02d",
				1900 + tm.tm_year, 1 + tm.tm_mon, tm.tm_mday);

			return buffer;
		}

		void exec(const std::string& sql)
		{
			char* errmsg{};

			if (sqlite3_exec(_db, sql.c_str(), nullptr, nullptr, &errmsg) != SQLITE_OK)
			{
				const std::string message{ errmsg != nullptr ? errmsg : "unknown error" };
				sqlite3_free(errmsg);

				throw std::runtime_error("sqlite3_exec failed: " + message);
			}
		}

		void ensureTableForDay(const std::tm& tm)
		{
			auto table{ tableNameForDay(tm) };

			if (table == _currentTable)
			{
				return;
			}

			exec("CREATE TABLE IF NOT EXISTS \"" + table + "\" ("
				"ts INTEGER NOT NULL," // unix time in ms
				"section TEXT NOT NULL,"
				"latency_ms REAL," // NULL when the ping was lost
				"error_code INTEGER NOT NULL,"
				"status_code INTEGER NOT NULL,"
				"responder TEXT,"
				"sys_latency_ms INTEGER);");

			exec("CREATE INDEX IF NOT EXISTS \"idx_" + table + "_ts\" "
				"ON \"" + table + "\"(ts);");

			if (_insertStmt != nullptr)
			{
				sqlite3_finalize(_insertStmt);
				_insertStmt = nullptr;
			}

			const auto sql{ "INSERT INTO \"" + table + "\" "
				"(ts, section, latency_ms, error_code, status_code, "
				"responder, sys_latency_ms) VALUES (?, ?, ?, ?, ?, ?, ?);" };

			if (sqlite3_prepare_v2(_db, sql.c_str(), -1, &_insertStmt, nullptr) != SQLITE_OK)
			{
				throw std::runtime_error(
					"Failed to prepare insert statement: "s + sqlite3_errmsg(_db));
			}

			_currentTable = std::move(table);

			// Cheap to run once per day-rollover; a no-op the rest of the time.
			pruneOldPartitions();
		}

		// All partition tables, oldest first - the names sort by date.
		std::vector<std::string> partitionTables()
		{
			std::vector<std::string> tables;

			sqlite3_stmt* stmt{};

			static constexpr auto SQL{
				"SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;" };

			if (sqlite3_prepare_v2(_db, SQL, -1, &stmt, nullptr) != SQLITE_OK)
			{
				return tables;
			}

			while (sqlite3_step(stmt) == SQLITE_ROW)
			{
				const auto name{ reinterpret_cast<const char*>(
					sqlite3_column_text(stmt, 0)) };

				if (name != nullptr && isPartitionTableName(name))
				{
					tables.emplace_back(name);
				}
			}

			sqlite3_finalize(stmt);

			return tables;
		}

		std::vector<std::string> partitionTablesSince(std::int64_t unixTimeMs)
		{
			const auto stamp{ static_cast<std::time_t>(unixTimeMs / 1000) };

			std::tm tm;
			localtime_s(&tm, &stamp);

			const auto firstTable{ tableNameForDay(tm) };

			auto tables{ partitionTables() };

			tables.erase(std::remove_if(tables.begin(), tables.end(),
				[&](const std::string& name) { return name < firstTable; }),
				tables.end());

			return tables;
		}

		void pruneOldPartitions()
		{
			static constexpr auto SECONDS_PER_DAY{ 24 * 3600 };

			const auto cutoff{ std::time(nullptr) - _retentionDays * SECONDS_PER_DAY };

			std::tm cutoffTm;
			localtime_s(&cutoffTm, &cutoff);

			const auto cutoffTable{ tableNameForDay(cutoffTm) };

			for (auto& table : partitionTables())
			{
				if (table < cutoffTable)
				{
					exec("DROP TABLE IF EXISTS \"" + table + "\";");
				}
			}
		}

		static bool isPartitionTableName(const std::string& name)
		{
			static constexpr char PREFIX[]{ "ping_log_" };
			static constexpr auto PREFIX_LEN{ sizeof(PREFIX) - 1 };

			return name.size() == PREFIX_LEN + 8 &&
				name.compare(0, PREFIX_LEN, PREFIX) == 0 &&
				std::all_of(name.begin() + PREFIX_LEN, name.end(),
					[](char c) { return std::isdigit(static_cast<unsigned char>(c)) != 0; });
		}
	};
}
