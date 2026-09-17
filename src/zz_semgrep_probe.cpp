// Temporary: verifies that the PR check's semgrep rules actually run on C++.
#include <cstdio>
#include <cstring>
#include <cstdlib>

void probe(char* input)
{
	char buffer[16];
	gets(buffer);
	strcpy(buffer, input);
	sprintf(buffer, "%s", input);
	system(input);
}
