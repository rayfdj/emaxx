/* A real loadable extension: the control must execute its registered code. */
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

static void probe(sqlite3_context *context, int argc, sqlite3_value **argv)
{
  (void) argc;
  int enabled = -1;
  if (sqlite3_db_config(sqlite3_context_db_handle(context),
                        SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, -1, &enabled)
        != SQLITE_OK || enabled != 0)
    {
      sqlite3_result_error(context, "C-API extension loading leaked", -1);
      return;
    }
  sqlite3_result_int64(context, sqlite3_value_int64(argv[0]) + 17);
}

int sqlite3_pcre_init(sqlite3 *db, char **error,
                     const sqlite3_api_routines *api)
{
  (void) error;
  SQLITE_EXTENSION_INIT2(api);
  return sqlite3_create_function(db, "extension_probe", 1,
                                SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                                0, probe, 0, 0);
}
