<?php
/**
 *
 * Idiorm
 *
 * http://github.com/j4mie/idiorm/
 *
 * A single-class super-simple database abstraction layer for PHP.
 * Provides (nearly) zero-configuration object-relational mapping
 * and a fluent interface for building basic, commonly-used queries.
 *
 * BSD Licensed.
 *
 * Copyright (c) 2010, Jamie Matthews
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */
namespace Idiorm;

use ArrayAccess;
use Exception;
use PDO;

class ORM implements ArrayAccess
{
    // ----------------------- //
    // --- CLASS CONSTANTS --- //
    // ----------------------- //
    // WHERE and HAVING condition array keys
    const CONDITION_FRAGMENT = 0;
    const CONDITION_VALUES = 1;
    const DEFAULT_CONNECTION = 'default';
    // Limit clause style
    const LIMIT_STYLE_TOP_N = "top";
    const LIMIT_STYLE_LIMIT = "limit";

    // ------------------------ //
    // --- CLASS PROPERTIES --- //
    // ------------------------ //
    // Class configuration
    protected static $default_config = [
        'connection_string' => 'sqlite::memory:',
        'id_column' => 'id',
        'id_column_overrides' => [],
        'error_mode' => PDO::ERRMODE_EXCEPTION,
        'username' => null,
        'password' => null,
        'driver_options' => null,
        'identifier_quote_character' => null, // if this is null, will be autodetected
        'limit_clause_style' => null, // if this is null, will be autodetected
        'logging' => false,
        'logger' => null,
        'caching' => false,
        'caching_auto_clear' => false,
        'return_result_sets' => false,
    ];
    // Map of configuration settings
    protected static $config = [];
    // Map of database connections, instances of the PDO class
    protected static $db = [];
    // Last query run, only populated if logging is enabled
    protected static $last_query;
    // Log of all queries run, mapped by connection key, only populated if logging is enabled
    protected static $query_log = [];
    // Query cache, only used if query caching is enabled
    protected static $query_cache = [];
    // Reference to previously used PDOStatement object to enable low-level access, if needed
    protected static $last_statement = null;
    // --------------------------- //
    // --- INSTANCE PROPERTIES --- //
    // --------------------------- //
    // Key name of the connections in static::$db used by this instance
    protected $connection_name;
    // The name of the table the current ORM instance is associated with
    protected $table_name;
    // Alias for the table to be used in SELECT queries
    protected $table_alias = null;
    // Values to be bound to the query
    protected $values = [];
    // Columns to select in the result
    protected $result_columns = ['*'];
    // Are we using the default result column or have these been manually changed?
    protected $using_default_result_columns = true;
    // Join sources
    protected $join_sources = [];
    // Should the query include a DISTINCT keyword?
    protected $distinct = false;
    // Is this a raw query?
    protected $is_raw_query = false;
    // The raw query
    protected $raw_query = '';
    // The raw query parameters
    protected $raw_parameters = [];
    // Array of WHERE clauses
    protected $where_conditions = [];
    // LIMIT
    protected $limit = null;
    // OFFSET
    protected $offset = null;
    // ORDER BY
    protected $order_by = [];
    // GROUP BY
    protected $group_by = [];
    // HAVING
    protected $having_conditions = [];
    // The data for a hydrated instance of the class
    protected $data = [];
    // Fields that have been modified during the
    // lifetime of the object
    protected $dirty_fields = [];
    // Fields that are to be inserted in the DB raw
    protected $expr_fields = [];
    // Is this a new object (has create() been called)?
    protected $is_new = false;
    // Name of the column to use as the primary key for
    // this instance only. Overrides the config settings.
    protected $instance_id_column = null;

    // ---------------------- //
    // --- STATIC METHODS --- //
    // ---------------------- //

    /**
     * Pass configuration settings to the class in the form of
     * key/value pairs. As a shortcut, if the second argument
     * is omitted and the key is a string, the setting is
     * assumed to be the DSN string used by PDO to connect
     * to the database (often, this will be the only configuration
     * required to use Idiorm). If you have more than one setting
     * you wish to configure, another shortcut is to pass an array
     * of settings (and omit the second argument).
     * @param string $key
     * @param mixed $value
     * @param string $connection_name Which connection to use
     */
    public static function configure($key, $value = null, $connection_name = self::DEFAULT_CONNECTION)
    {
        static::setupDbConfig($connection_name); //ensures at least default config is set

        if (is_array($key)) {
            // Shortcut: If only one array argument is passed,
            // assume it's an array of configuration settings
            foreach ($key as $conf_key => $conf_value) {
                static::configure($conf_key, $conf_value, $connection_name);
            }
        } else {
            if (is_null($value)) {
                // Shortcut: If only one string argument is passed, 
                // assume it's a connection string
                $value = $key;
                $key = 'connection_string';
            }
            static::$config[$connection_name][$key] = $value;
        }
    }

    /**
     * Retrieve configuration options by key, or as whole array.
     * @param string $key
     * @param string $connection_name Which connection to use
     */
    public static function getConfig($key = null, $connection_name = self::DEFAULT_CONNECTION)
    {
        if ($key) {
            return static::$config[$connection_name][$key];
        } else {
            return static::$config[$connection_name];
        }
    }

    /**
     * Delete all configs in config array.
     */
    public static function resetConfig()
    {
        static::$config = [];
    }

    /**
     * Despite its slightly odd name, this is actually the factory
     * method used to acquire instances of the class. It is named
     * this way for the sake of a readable interface, ie
     * ORM::forTable('table_name')->findOne()-> etc. As such,
     * this will normally be the first method called in a chain.
     * @param string $table_name
     * @param string $connection_name Which connection to use
     * @return ORM
     */
    public static function forTable($table_name, $connection_name = self::DEFAULT_CONNECTION)
    {
        static::setupDb($connection_name);
        return new static($table_name, [], $connection_name);
    }

    /**
     * Set up the database connection used by the class
     * @param string $connection_name Which connection to use
     */
    protected static function setupDb($connection_name = self::DEFAULT_CONNECTION)
    {
        if (!array_key_exists($connection_name, static::$db) ||
            !is_object(static::$db[$connection_name])) {
            static::setupDbConfig($connection_name);

            $db = new PDO(
                static::$config[$connection_name]['connection_string'], static::$config[$connection_name]['username'], static::$config[$connection_name]['password'], static::$config[$connection_name]['driver_options']
            );

            $db->setAttribute(PDO::ATTR_ERRMODE, static::$config[$connection_name]['error_mode']);
            static::setDb($db, $connection_name);
        }
    }

    /**
     * Ensures configuration (multiple connections) is at least set to default.
     * @param string $connection_name Which connection to use
     */
    protected static function setupDbConfig($connection_name)
    {
        if (!array_key_exists($connection_name, static::$config)) {
            static::$config[$connection_name] = static::$default_config;
        }
    }

    /**
     * Set the PDO object used by Idiorm to communicate with the database.
     * This is public in case the ORM should use a ready-instantiated
     * PDO object as its database connection. Accepts an optional string key
     * to identify the connection if multiple connections are used.
     * @param PDO $db
     * @param string $connection_name Which connection to use
     */
    public static function setDb($db, $connection_name = self::DEFAULT_CONNECTION)
    {
        static::setupDbConfig($connection_name);
        static::$db[$connection_name] = $db;
        if (!is_null(static::$db[$connection_name])) {
            static::setupIdentifierQuoteCharacter($connection_name);
            static::setupLimitClauseStyle($connection_name);
        }
    }

    /**
     * Delete all registered PDO objects in db array.
     */
    public static function resetDb()
    {
        static::$db = [];
    }

    /**
     * Detect and initialise the character used to quote identifiers
     * (table names, column names etc). If this has been specified
     * manually using ORM::configure('identifier_quote_character', 'some-char'),
     * this will do nothing.
     * @param string $connection_name Which connection to use
     */
    protected static function setupIdentifierQuoteCharacter($connection_name)
    {
        if (is_null(static::$config[$connection_name]['identifier_quote_character'])) {
            static::$config[$connection_name]['identifier_quote_character'] = static::detectIdentifierQuoteCharacter($connection_name);
        }
    }

    /**
     * Detect and initialise the limit clause style ("SELECT TOP 5" /
     * "... LIMIT 5"). If this has been specified manually using 
     * ORM::configure('limit_clause_style', 'top'), this will do nothing.
     * @param string $connection_name Which connection to use
     */
    public static function setupLimitClauseStyle($connection_name)
    {
        if (is_null(static::$config[$connection_name]['limit_clause_style'])) {
            static::$config[$connection_name]['limit_clause_style'] = static::detectLimitClauseStyle($connection_name);
        }
    }

    /**
     * Return the correct character used to quote identifiers (table
     * names, column names etc) by looking at the driver being used by PDO.
     * @param string $connection_name Which connection to use
     * @return string
     */
    protected static function detectIdentifierQuoteCharacter($connection_name)
    {
        switch (static::getDb($connection_name)->getAttribute(PDO::ATTR_DRIVER_NAME)) {
            case 'pgsql':
            case 'sqlsrv':
            case 'dblib':
            case 'mssql':
            case 'sybase':
            case 'firebird':
                return '"';
            case 'mysql':
            case 'sqlite':
            case 'sqlite2':
            default:
                return '`';
        }
    }

    /**
     * Returns a constant after determining the appropriate limit clause
     * style
     * @param string $connection_name Which connection to use
     * @return string Limit clause style keyword/constant
     */
    protected static function detectLimitClauseStyle($connection_name)
    {
        switch (static::getDb($connection_name)->getAttribute(PDO::ATTR_DRIVER_NAME)) {
            case 'sqlsrv':
            case 'dblib':
            case 'mssql':
                return ORM::LIMIT_STYLE_TOP_N;
            default:
                return ORM::LIMIT_STYLE_LIMIT;
        }
    }

    /**
     * Returns the PDO instance used by the the ORM to communicate with
     * the database. This can be called if any low-level DB access is
     * required outside the class. If multiple connections are used,
     * accepts an optional key name for the connection.
     * @param string $connection_name Which connection to use
     * @return PDO
     */
    public static function getDb($connection_name = self::DEFAULT_CONNECTION)
    {
        static::setupDb($connection_name); // required in case this is called before Idiorm is instantiated
        return static::$db[$connection_name];
    }

    /**
     * Executes a raw query as a wrapper for PDOStatement::execute.
     * Useful for queries that can't be accomplished through Idiorm,
     * particularly those using engine-specific features.
     * @example rawExecute('SELECT `name`, AVG(`order`) FROM `customer` GROUP BY `name` HAVING AVG(`order`) > 10')
     * @example rawExecute('INSERT OR REPLACE INTO `widget` (`id`, `name`) SELECT `id`, `name` FROM `other_table`')
     * @param string $query The raw SQL query
     * @param array  $parameters Optional bound parameters
     * @param string $connection_name Which connection to use
     * @return bool Success
     */
    public static function rawExecute($query, $parameters = [], $connection_name = self::DEFAULT_CONNECTION)
    {
        static::setupDb($connection_name);
        return static::execute($query, $parameters, $connection_name);
    }

    /**
     * Returns the PDOStatement instance last used by any connection wrapped by the ORM.
     * Useful for access to PDOStatement::rowCount() or error information
     * @return PDOStatement
     */
    public static function getLastStatement()
    {
        return static::$last_statement;
    }

    /**
     * Internal helper method for executing statments. Logs queries, and
     * stores statement object in ::_last_statment, accessible publicly
     * through ::getLastStatement()
     * @param string $query
     * @param array $parameters An array of parameters to be bound in to the query
     * @param string $connection_name Which connection to use
     * @return bool Response of PDOStatement::execute()
     */
    protected static function execute($query, $parameters = [], $connection_name = self::DEFAULT_CONNECTION)
    {
        $statement = static::getDb($connection_name)->prepare($query);
        static::$last_statement = $statement;
        $time = microtime(true);

        foreach ($parameters as $key => &$param) {
            if (is_null($param)) {
                $type = PDO::PARAM_NULL;
            } else if (is_bool($param)) {
                $type = PDO::PARAM_BOOL;
            } else if (is_int($param)) {
                $type = PDO::PARAM_INT;
            } else {
                $type = PDO::PARAM_STR;
            }

            $statement->bindParam(is_int($key) ? ++$key : $key, $param, $type);
        }

        $q = $statement->execute();
        static::logQuery($query, $parameters, $connection_name, (microtime(true) - $time));

        return $q;
    }

    /**
     * Add a query to the internal query log. Only works if the
     * 'logging' config option is set to true.
     *
     * This works by manually binding the parameters to the query - the
     * query isn't executed like this (PDO normally passes the query and
     * parameters to the database which takes care of the binding) but
     * doing it this way makes the logged queries more readable.
     * @param string $query
     * @param array $parameters An array of parameters to be bound in to the query
     * @param string $connection_name Which connection to use
     * @param float $query_time Query time
     * @return bool
     */
    protected static function logQuery($query, $parameters, $connection_name, $query_time)
    {
        // If logging is not enabled, do nothing
        if (!static::$config[$connection_name]['logging']) {
            return false;
        }

        if (!isset(static::$query_log[$connection_name])) {
            static::$query_log[$connection_name] = [];
        }

        // Strip out any non-integer indexes from the parameters
        foreach ($parameters as $key => $value) {
            if (!is_int($key))
                unset($parameters[$key]);
        }

        if (count($parameters) > 0) {
            // Escape the parameters
            $parameters = array_map([static::getDb($connection_name), 'quote'], $parameters);

            // Avoid %format collision for vsprintf
            $query = str_replace("%", "%%", $query);

            // Replace placeholders in the query for vsprintf
            if (false !== strpos($query, "'") || false !== strpos($query, '"')) {
                $query = StringReplace::strReplaceOutsideQuotes("?", "%s", $query);
            } else {
                $query = str_replace("?", "%s", $query);
            }

            // Replace the question marks in the query with the parameters
            $bound_query = vsprintf($query, $parameters);
        } else {
            $bound_query = $query;
        }

        static::$last_query = $bound_query;
        static::$query_log[$connection_name][] = $bound_query;


        if (is_callable(static::$config[$connection_name]['logger'])) {
            $logger = static::$config[$connection_name]['logger'];
            $logger($bound_query, $query_time);
        }

        return true;
    }

    /**
     * Get the last query executed. Only works if the
     * 'logging' config option is set to true. Otherwise
     * this will return null. Returns last query from all connections if
     * no connection_name is specified
     * @param null|string $connection_name Which connection to use
     * @return string
     */
    public static function getLastQuery($connection_name = null)
    {
        if ($connection_name === null) {
            return static::$last_query;
        }
        if (!isset(static::$query_log[$connection_name])) {
            return '';
        }

        return end(static::$query_log[$connection_name]);
    }

    /**
     * Get an array containing all the queries run on a
     * specified connection up to now.
     * Only works if the 'logging' config option is
     * set to true. Otherwise, returned array will be empty.
     * @param string $connection_name Which connection to use
     */
    public static function getQueryLog($connection_name = self::DEFAULT_CONNECTION)
    {
        if (isset(static::$query_log[$connection_name])) {
            return static::$query_log[$connection_name];
        }
        return [];
    }

    /**
     * Get a list of the available connection names
     * @return array
     */
    public static function getConnectionNames()
    {
        return array_keys(static::$db);
    }

    // ------------------------ //
    // --- INSTANCE METHODS --- //
    // ------------------------ //

    /**
     * "Private" constructor; shouldn't be called directly.
     * Use the ORM::table factory method instead.
     */
    protected function __construct($table_name, $data = [], $connection_name = self::DEFAULT_CONNECTION)
    {
        $this->table_name = $table_name;
        $this->data = $data;

        $this->connection_name = $connection_name;
        static::setupDbConfig($connection_name);
    }

    /**
     * Create a new, empty instance of the class. Used
     * to add a new row to your database. May optionally
     * be passed an associative array of data to populate
     * the instance. If so, all fields will be flagged as
     * dirty so all will be saved to the database when
     * save() is called.
     */
    public function create($data = null)
    {
        $this->is_new = true;
        if (!is_null($data)) {
            return $this->hydrate($data)->forceAllDirty();
        }
        return $this;
    }

    /**
     * Specify the ID column to use for this instance or array of instances only.
     * This overrides the id_column and id_column_overrides settings.
     *
     * This is mostly useful for libraries built on top of Idiorm, and will
     * not normally be used in manually built queries. If you don't know why
     * you would want to use this, you should probably just ignore it.
     */
    public function useIdColumn($id_column)
    {
        $this->instance_id_column = $id_column;
        return $this;
    }

    /**
     * Create an ORM instance from the given row (an associative
     * array of data fetched from the database)
     */
    protected function createInstanceFromRow($row)
    {
        $instance = static::forTable($this->table_name, $this->connection_name);
        $instance->useIdColumn($this->instance_id_column);
        $instance->hydrate($row);
        return $instance;
    }

    /**
     * Tell the ORM that you are expecting a single result
     * back from your query, and execute it. Will return
     * a single instance of the ORM class, or false if no
     * rows were returned.
     * As a shortcut, you may supply an ID as a parameter
     * to this method. This will perform a primary key
     * lookup on the table.
     */
    public function findOne($id = null)
    {
        if (!is_null($id)) {
            $this->whereIdIs($id);
        }
        $this->limit(1);
        $rows = $this->run();

        if (empty($rows)) {
            return false;
        }

        return $this->createInstanceFromRow($rows[0]);
    }

    /**
     * Tell the ORM that you are expecting multiple results
     * from your query, and execute it. Will return an array
     * of instances of the ORM class, or an empty array if
     * no rows were returned.
     * @return array|\ResultSet
     */
    public function findMany()
    {
        if (static::$config[$this->connection_name]['return_result_sets']) {
            return $this->findResultSet();
        }
        return $this->findManyArray();
    }

    /**
     * Tell the ORM that you are expecting multiple results
     * from your query, and execute it. Will return an array
     * of instances of the ORM class, or an empty array if
     * no rows were returned.
     * @return array
     */
    protected function findManyArray()
    {
        $rows = $this->run();
        return array_map([$this, 'createInstanceFromRow'], $rows);
    }

    /**
     * Tell the ORM that you are expecting multiple results
     * from your query, and execute it. Will return a result set object
     * containing instances of the ORM class.
     * @return \ResultSet
     */
    public function findResultSet()
    {
        return new ResultSet($this->findManyArray());
    }

    /**
     * Tell the ORM that you are expecting multiple results
     * from your query, and execute it. Will return an array,
     * or an empty array if no rows were returned.
     * @return array
     */
    public function findArray()
    {
        return $this->run();
    }

    /**
     * Tell the ORM that you wish to execute a COUNT query.
     * Will return an integer representing the number of
     * rows returned.
     */
    public function count($column = '*')
    {
        return $this->callAggregateDbFunction(__FUNCTION__, $column);
    }

    /**
     * Tell the ORM that you wish to execute a MAX query.
     * Will return the max value of the choosen column.
     */
    public function max($column)
    {
        return $this->callAggregateDbFunction(__FUNCTION__, $column);
    }

    /**
     * Tell the ORM that you wish to execute a MIN query.
     * Will return the min value of the choosen column.
     */
    public function min($column)
    {
        return $this->callAggregateDbFunction(__FUNCTION__, $column);
    }

    /**
     * Tell the ORM that you wish to execute a AVG query.
     * Will return the average value of the choosen column.
     */
    public function avg($column)
    {
        return $this->callAggregateDbFunction(__FUNCTION__, $column);
    }

    /**
     * Tell the ORM that you wish to execute a SUM query.
     * Will return the sum of the choosen column.
     */
    public function sum($column)
    {
        return $this->callAggregateDbFunction(__FUNCTION__, $column);
    }

    /**
     * Execute an aggregate query on the current connection.
     * @param string $sql_function The aggregate function to call eg. MIN, COUNT, etc
     * @param string $column The column to execute the aggregate query against
     * @return int
     */
    protected function callAggregateDbFunction($sql_function, $column)
    {
        $alias = strtolower($sql_function);
        $sql_function = strtoupper($sql_function);
        if ('*' != $column) {
            $column = $this->quoteIdentifier($column);
        }
        $result_columns = $this->result_columns;
        $this->result_columns = [];
        $this->selectExpr("$sql_function($column)", $alias);
        $result = $this->findOne();
        $this->result_columns = $result_columns;

        $return_value = 0;
        if ($result !== false && isset($result->$alias)) {
            if (!is_numeric($result->$alias)) {
                $return_value = $result->$alias;
            } elseif ((int) $result->$alias == (float) $result->$alias) {
                $return_value = (int) $result->$alias;
            } else {
                $return_value = (float) $result->$alias;
            }
        }
        return $return_value;
    }

    /**
     * This method can be called to hydrate (populate) this
     * instance of the class from an associative array of data.
     * This will usually be called only from inside the class,
     * but it's public in case you need to call it directly.
     */
    public function hydrate($data = [])
    {
        $this->data = $data;
        return $this;
    }

    /**
     * Force the ORM to flag all the fields in the $data array
     * as "dirty" and therefore update them when save() is called.
     */
    public function forceAllDirty()
    {
        $this->dirty_fields = $this->data;
        return $this;
    }

    /**
     * Perform a raw query. The query can contain placeholders in
     * either named or question mark style. If placeholders are
     * used, the parameters should be an array of values which will
     * be bound to the placeholders in the query. If this method
     * is called, all other query building methods will be ignored.
     */
    public function rawQuery($query, $parameters = [])
    {
        $this->is_raw_query = true;
        $this->raw_query = $query;
        $this->raw_parameters = $parameters;
        return $this;
    }

    /**
     * Add an alias for the main table to be used in SELECT queries
     */
    public function tableAlias($alias)
    {
        $this->table_alias = $alias;
        return $this;
    }

    /**
     * Internal method to add an unquoted expression to the set
     * of columns returned by the SELECT query. The second optional
     * argument is the alias to return the expression as.
     */
    protected function addResultColumn($expr, $alias = null)
    {
        if (!is_null($alias)) {
            $expr .= " AS " . $this->quoteIdentifier($alias);
        }

        if ($this->using_default_result_columns) {
            $this->result_columns = [$expr];
            $this->using_default_result_columns = false;
        } else {
            $this->result_columns[] = $expr;
        }
        return $this;
    }

    /**
     * Counts the number of columns that belong to the primary
     * key and their value is null.
     */
    public function countNullIdColumns()
    {
        if (is_array($this->getIdColumnName())) {
            return count(array_filter($this->id(), 'is_null'));
        } else {
            return is_null($this->id()) ? 1 : 0;
        }
    }

    /**
     * Add a column to the list of columns returned by the SELECT
     * query. This defaults to '*'. The second optional argument is
     * the alias to return the column as.
     */
    public function select($column, $alias = null)
    {
        $column = $this->quoteIdentifier($column);
        return $this->addResultColumn($column, $alias);
    }

    /**
     * Add an unquoted expression to the list of columns returned
     * by the SELECT query. The second optional argument is
     * the alias to return the column as.
     */
    public function selectExpr($expr, $alias = null)
    {
        return $this->addResultColumn($expr, $alias);
    }

    /**
     * Add columns to the list of columns returned by the SELECT
     * query. This defaults to '*'. Many columns can be supplied
     * as either an array or as a list of parameters to the method.
     * 
     * Note that the alias must not be numeric - if you want a
     * numeric alias then prepend it with some alpha chars. eg. a1
     * 
     * @example select_many(['alias' => 'column', 'column2', 'alias2' => 'column3'], 'column4', 'column5');
     * @example select_many('column', 'column2', 'column3');
     * @example select_many(['column', 'column2', 'column3'], 'column4', 'column5');
     * 
     * @return \ORM
     */
    public function selectMany()
    {
        $columns = func_get_args();
        if (!empty($columns)) {
            $columns = $this->normaliseSelectManyColumns($columns);
            foreach ($columns as $alias => $column) {
                if (is_numeric($alias)) {
                    $alias = null;
                }
                $this->select($column, $alias);
            }
        }
        return $this;
    }

    /**
     * Add an unquoted expression to the list of columns returned
     * by the SELECT query. Many columns can be supplied as either 
     * an array or as a list of parameters to the method.
     * 
     * Note that the alias must not be numeric - if you want a
     * numeric alias then prepend it with some alpha chars. eg. a1
     * 
     * @example selectManyExpr(['alias' => 'column', 'column2', 'alias2' => 'column3'], 'column4', 'column5')
     * @example selectManyExpr('column', 'column2', 'column3')
     * @example selectManyExpr(['column', 'column2', 'column3'], 'column4', 'column5')
     * 
     * @return \ORM
     */
    public function selectManyExpr()
    {
        $columns = func_get_args();
        if (!empty($columns)) {
            $columns = $this->normaliseSelectManyColumns($columns);
            foreach ($columns as $alias => $column) {
                if (is_numeric($alias)) {
                    $alias = null;
                }
                $this->selectExpr($column, $alias);
            }
        }
        return $this;
    }

    /**
     * Take a column specification for the select many methods and convert it
     * into a normalised array of columns and aliases.
     * 
     * It is designed to turn the following styles into a normalised array:
     * 
     * [['alias' => 'column', 'column2', 'alias2' => 'column3'], 'column4', 'column5')]
     * 
     * @param array $columns
     * @return array
     */
    protected function normaliseSelectManyColumns($columns)
    {
        $return = [];
        foreach ($columns as $column) {
            if (is_array($column)) {
                foreach ($column as $key => $value) {
                    if (!is_numeric($key)) {
                        $return[$key] = $value;
                    } else {
                        $return[] = $value;
                    }
                }
            } else {
                $return[] = $column;
            }
        }
        return $return;
    }

    /**
     * Add a DISTINCT keyword before the list of columns in the SELECT query
     */
    public function distinct()
    {
        $this->distinct = true;
        return $this;
    }

    /**
     * Internal method to add a JOIN source to the query.
     *
     * The join_operator should be one of INNER, LEFT OUTER, CROSS etc - this
     * will be prepended to JOIN.
     *
     * The table should be the name of the table to join to.
     *
     * The constraint may be either a string or an array with three elements. If it
     * is a string, it will be compiled into the query as-is, with no escaping. The
     * recommended way to supply the constraint is as an array with three elements:
     *
     * first_column, operator, second_column
     *
     * Example: ['user.id', '=', 'profile.user_id']
     *
     * will compile to
     *
     * ON `user`.`id` = `profile`.`user_id`
     *
     * The final (optional) argument specifies an alias for the joined table.
     */
    protected function addJoinSource($join_operator, $table, $constraint, $table_alias = null)
    {

        $join_operator = trim("{$join_operator} JOIN");

        $table = $this->quoteIdentifier($table);

        // Add table alias if present
        if (!is_null($table_alias)) {
            $table_alias = $this->quoteIdentifier($table_alias);
            $table .= " {$table_alias}";
        }

        // Build the constraint
        if (is_array($constraint)) {
            list($first_column, $operator, $second_column) = $constraint;
            $first_column = $this->quoteIdentifier($first_column);
            $second_column = $this->quoteIdentifier($second_column);
            $constraint = "{$first_column} {$operator} {$second_column}";
        }

        $this->join_sources[] = "{$join_operator} {$table} ON {$constraint}";
        return $this;
    }

    /**
     * Add a RAW JOIN source to the query
     */
    public function rawJoin($table, $constraint, $table_alias, $parameters = [])
    {
        // Add table alias if present
        if (!is_null($table_alias)) {
            $table_alias = $this->quoteIdentifier($table_alias);
            $table .= " {$table_alias}";
        }

        $this->values = array_merge($this->values, $parameters);

        // Build the constraint
        if (is_array($constraint)) {
            list($first_column, $operator, $second_column) = $constraint;
            $first_column = $this->quoteIdentifier($first_column);
            $second_column = $this->quoteIdentifier($second_column);
            $constraint = "{$first_column} {$operator} {$second_column}";
        }

        $this->join_sources[] = "{$table} ON {$constraint}";
        return $this;
    }

    /**
     * Add a simple JOIN source to the query
     */
    public function join($table, $constraint, $table_alias = null)
    {
        return $this->addJoinSource("", $table, $constraint, $table_alias);
    }

    /**
     * Add an INNER JOIN souce to the query
     */
    public function innerJoin($table, $constraint, $table_alias = null)
    {
        return $this->addJoinSource("INNER", $table, $constraint, $table_alias);
    }

    /**
     * Add a LEFT OUTER JOIN souce to the query
     */
    public function leftOuterJoin($table, $constraint, $table_alias = null)
    {
        return $this->addJoinSource("LEFT OUTER", $table, $constraint, $table_alias);
    }

    /**
     * Add an RIGHT OUTER JOIN souce to the query
     */
    public function rightOuterJoin($table, $constraint, $table_alias = null)
    {
        return $this->addJoinSource("RIGHT OUTER", $table, $constraint, $table_alias);
    }

    /**
     * Add an FULL OUTER JOIN souce to the query
     */
    public function fullOuterJoin($table, $constraint, $table_alias = null)
    {
        return $this->addJoinSource("FULL OUTER", $table, $constraint, $table_alias);
    }

    /**
     * Internal method to add a HAVING condition to the query
     */
    protected function addHaving($fragment, $values = [])
    {
        return $this->addCondition('having', $fragment, $values);
    }

    /**
     * Internal method to add a HAVING condition to the query
     */
    protected function addSimpleHaving($column_name, $separator, $value)
    {
        return $this->addSimpleCondition('having', $column_name, $separator, $value);
    }

    /**
     * Internal method to add a HAVING clause with multiple values (like IN and NOT IN)
     */
    public function addHavingPlaceholder($column_name, $separator, $values)
    {
        if (!is_array($column_name)) {
            $data = [$column_name => $values];
        } else {
            $data = $column_name;
        }
        $result = $this;
        foreach ($data as $key => $val) {
            $column = $result->quoteIdentifier($key);
            $placeholders = $result->createPlaceholders($val);
            $result = $result->addHaving("{$column} {$separator} ({$placeholders})", $val);
        }
        return $result;
    }

    /**
     * Internal method to add a HAVING clause with no parameters(like IS NULL and IS NOT NULL)
     */
    public function addHavingNoValue($column_name, $operator)
    {
        $conditions = (is_array($column_name)) ? $column_name : [$column_name];
        $result = $this;
        foreach ($conditions as $column) {
            $column = $this->quoteIdentifier($column);
            $result = $result->addHaving("{$column} {$operator}");
        }
        return $result;
    }

    /**
     * Internal method to add a WHERE condition to the query
     */
    protected function addWhere($fragment, $values = [])
    {
        return $this->addCondition('where', $fragment, $values);
    }

    /**
     * Internal method to add a WHERE condition to the query
     */
    protected function addSimpleWhere($column_name, $separator, $value)
    {
        return $this->addSimpleCondition('where', $column_name, $separator, $value);
    }

    /**
     * Add a WHERE clause with multiple values (like IN and NOT IN)
     */
    public function addWherePlaceholder($column_name, $separator, $values)
    {
        if (!is_array($column_name)) {
            $data = [$column_name => $values];
        } else {
            $data = $column_name;
        }
        $result = $this;
        foreach ($data as $key => $val) {
            $column = $result->quoteIdentifier($key);
            $placeholders = $result->createPlaceholders($val);
            $result = $result->addWhere("{$column} {$separator} ({$placeholders})", $val);
        }
        return $result;
    }

    /**
     * Add a WHERE clause with no parameters(like IS NULL and IS NOT NULL)
     */
    public function addWhereNoValue($column_name, $operator)
    {
        $conditions = (is_array($column_name)) ? $column_name : [$column_name];
        $result = $this;
        foreach ($conditions as $column) {
            $column = $this->quoteIdentifier($column);
            $result = $result->addWhere("{$column} {$operator}");
        }
        return $result;
    }

    /**
     * Internal method to add a HAVING or WHERE condition to the query
     */
    protected function addCondition($type, $fragment, $values = [])
    {
        $conditions_class_property_name = "{$type}_conditions";
        if (!is_array($values)) {
            $values = [$values];
        }
        array_push($this->$conditions_class_property_name, [
            static::CONDITION_FRAGMENT => $fragment,
            static::CONDITION_VALUES => $values,
        ]);
        return $this;
    }

    /**
     * Helper method to compile a simple COLUMN SEPARATOR VALUE
     * style HAVING or WHERE condition into a string and value ready to
     * be passed to the _addCondition method. Avoids duplication
     * of the call to quoteIdentifier
     *
     * If column_name is an associative array, it will add a condition for each column
     */
    protected function addSimpleCondition($type, $column_name, $separator, $value)
    {
        $multiple = is_array($column_name) ? $column_name : [$column_name => $value];
        $result = $this;

        foreach ($multiple as $key => $val) {
            // Add the table name in case of ambiguous columns
            if (count($result->join_sources) > 0 && strpos($key, '.') === false) {
                $table = $result->table_name;
                if (!is_null($result->table_alias)) {
                    $table = $result->table_alias;
                }

                $key = "{$table}.{$key}";
            }
            $key = $result->quoteIdentifier($key);
            $result = $result->addCondition($type, "{$key} {$separator} ?", $val);
        }
        return $result;
    }

    /**
     * Return a string containing the given number of question marks,
     * separated by commas. Eg "?, ?, ?"
     */
    protected function createPlaceholders($fields)
    {
        if (!empty($fields)) {
            $db_fields = [];
            foreach ($fields as $key => $value) {
                // Process expression fields directly into the query
                if (array_key_exists($key, $this->expr_fields)) {
                    $db_fields[] = $value;
                } else {
                    $db_fields[] = '?';
                }
            }
            return implode(', ', $db_fields);
        }
    }

    /**
     * Helper method that filters a column/value array returning only those
     * columns that belong to a compound primary key.
     *
     * If the key contains a column that does not exist in the given array,
     * a null value will be returned for it.
     */
    protected function getCompoundIdColumnValues($value)
    {
        $filtered = [];
        foreach ($this->getIdColumnName() as $key) {
            $filtered[$key] = isset($value[$key]) ? $value[$key] : null;
        }
        return $filtered;
    }

    /**
     * Helper method that filters an array containing compound column/value
     * arrays.
     */
    protected function getCompoundIdColumnValuesArray($values)
    {
        $filtered = [];
        foreach ($values as $value) {
            $filtered[] = $this->getCompoundIdColumnValues($value);
        }
        return $filtered;
    }

    /**
     * Add a WHERE column = value clause to your query. Each time
     * this is called in the chain, an additional WHERE will be
     * added, and these will be ANDed together when the final query
     * is built.
     *
     * If you use an array in $column_name, a new clause will be
     * added for each element. In this case, $value is ignored.
     */
    public function where($column_name, $value = null)
    {
        return $this->whereEqual($column_name, $value);
    }

    /**
     * More explicitly named version of for the where() method.
     * Can be used if preferred.
     */
    public function whereEqual($column_name, $value = null)
    {
        return $this->addSimpleWhere($column_name, '=', $value);
    }

    /**
     * Add a WHERE column != value clause to your query.
     */
    public function whereNotEqual($column_name, $value = null)
    {
        return $this->addSimpleWhere($column_name, '!=', $value);
    }

    /**
     * Special method to query the table by its primary key
     *
     * If primary key is compound, only the columns that
     * belong to they key will be used for the query
     */
    public function whereIdIs($id)
    {
        return (is_array($this->getIdColumnName())) ?
            $this->where($this->getCompoundIdColumnValues($id), null) :
            $this->where($this->getIdColumnName(), $id);
    }

    /**
     * Allows adding a WHERE clause that matches any of the conditions
     * specified in the array. Each element in the associative array will
     * be a different condition, where the key will be the column name.
     *
     * By default, an equal operator will be used against all columns, but
     * it can be overriden for any or every column using the second parameter.
     *
     * Each condition will be ORed together when added to the final query.
     */
    public function whereAnyIs($values, $operator = '=')
    {
        $data = [];
        $query = ["(("];
        $first = true;
        foreach ($values as $item) {
            if ($first) {
                $first = false;
            } else {
                $query[] = ") OR (";
            }
            $firstsub = true;
            foreach ($item as $key => $item) {
                $op = is_string($operator) ? $operator : (isset($operator[$key]) ? $operator[$key] : '=');
                if ($firstsub) {
                    $firstsub = false;
                } else {
                    $query[] = "AND";
                }
                $query[] = $this->quoteIdentifier($key);
                $data[] = $item;
                $query[] = $op . " ?";
            }
        }
        $query[] = "))";
        return $this->whereRaw(join($query, ' '), $data);
    }

    /**
     * Similar to whereIdIs() but allowing multiple primary keys.
     *
     * If primary key is compound, only the columns that
     * belong to they key will be used for the query
     */
    public function whereIdIn($ids)
    {
        return (is_array($this->getIdColumnName())) ?
            $this->whereAnyIs($this->getCompoundIdColumnValuesArray($ids)) :
            $this->whereIn($this->getIdColumnName(), $ids);
    }

    /**
     * Add a WHERE ... LIKE clause to your query.
     */
    public function whereLike($column_name, $value = null)
    {
        return $this->addSimpleWhere($column_name, 'LIKE', $value);
    }

    /**
     * Add where WHERE ... NOT LIKE clause to your query.
     */
    public function whereNotLike($column_name, $value = null)
    {
        return $this->addSimpleWhere($column_name, 'NOT LIKE', $value);
    }

    /**
     * Add a WHERE ... > clause to your query
     */
    public function whereGt($column_name, $value = null)
    {
        return $this->addSimpleWhere($column_name, '>', $value);
    }

    /**
     * Add a WHERE ... < clause to your query
     */
    public function whereLt($column_name, $value = null)
    {
        return $this->addSimpleWhere($column_name, '<', $value);
    }

    /**
     * Add a WHERE ... >= clause to your query
     */
    public function whereGte($column_name, $value = null)
    {
        return $this->addSimpleWhere($column_name, '>=', $value);
    }

    /**
     * Add a WHERE ... <= clause to your query
     */
    public function whereLte($column_name, $value = null)
    {
        return $this->addSimpleWhere($column_name, '<=', $value);
    }

    /**
     * Add a WHERE ... IN clause to your query
     */
    public function whereIn($column_name, $values)
    {
        return $this->addWherePlaceholder($column_name, 'IN', $values);
    }

    /**
     * Add a WHERE ... NOT IN clause to your query
     */
    public function whereNotIn($column_name, $values)
    {
        return $this->addWherePlaceholder($column_name, 'NOT IN', $values);
    }

    /**
     * Add a WHERE column IS NULL clause to your query
     */
    public function whereNull($column_name)
    {
        return $this->addWhereNoValue($column_name, "IS NULL");
    }

    /**
     * Add a WHERE column IS NOT NULL clause to your query
     */
    public function whereNotNull($column_name)
    {
        return $this->addWhereNoValue($column_name, "IS NOT NULL");
    }

    /**
     * Add a raw WHERE clause to the query. The clause should
     * contain question mark placeholders, which will be bound
     * to the parameters supplied in the second argument.
     */
    public function whereRaw($clause, $parameters = [])
    {
        return $this->addWhere($clause, $parameters);
    }

    /**
     * Add a LIMIT to the query
     */
    public function limit($limit)
    {
        $this->limit = $limit;
        return $this;
    }

    /**
     * Add an OFFSET to the query
     */
    public function offset($offset)
    {
        $this->offset = $offset;
        return $this;
    }

    /**
     * Add an ORDER BY clause to the query
     */
    protected function addOrderBy($column_name, $ordering)
    {
        $column_name = $this->quoteIdentifier($column_name);
        $this->order_by[] = "{$column_name} {$ordering}";
        return $this;
    }

    /**
     * Add an ORDER BY column DESC clause
     */
    public function orderByDesc($column_name)
    {
        return $this->addOrderBy($column_name, 'DESC');
    }

    /**
     * Add an ORDER BY column ASC clause
     */
    public function orderByAsc($column_name)
    {
        return $this->addOrderBy($column_name, 'ASC');
    }

    /**
     * Add an unquoted expression as an ORDER BY clause
     */
    public function orderByExpr($clause)
    {
        $this->order_by[] = $clause;
        return $this;
    }

    /**
     * Add a column to the list of columns to GROUP BY
     */
    public function groupBy($column_name)
    {
        $column_name = $this->quoteIdentifier($column_name);
        $this->group_by[] = $column_name;
        return $this;
    }

    /**
     * Add an unquoted expression to the list of columns to GROUP BY 
     */
    public function groupByExpr($expr)
    {
        $this->group_by[] = $expr;
        return $this;
    }

    /**
     * Add a HAVING column = value clause to your query. Each time
     * this is called in the chain, an additional HAVING will be
     * added, and these will be ANDed together when the final query
     * is built.
     *
     * If you use an array in $column_name, a new clause will be
     * added for each element. In this case, $value is ignored.
     */
    public function having($column_name, $value = null)
    {
        return $this->havingEqual($column_name, $value);
    }

    /**
     * More explicitly named version of for the having() method.
     * Can be used if preferred.
     */
    public function havingEqual($column_name, $value = null)
    {
        return $this->addSimpleHaving($column_name, '=', $value);
    }

    /**
     * Add a HAVING column != value clause to your query.
     */
    public function havingNotEqual($column_name, $value = null)
    {
        return $this->addSimpleHaving($column_name, '!=', $value);
    }

    /**
     * Special method to query the table by its primary key.
     *
     * If primary key is compound, only the columns that
     * belong to they key will be used for the query
     */
    public function havingIdIs($id)
    {
        return (is_array($this->getIdColumnName())) ?
            $this->having($this->getCompoundIdColumnValues($value)) :
            $this->having($this->getIdColumnName(), $id);
    }

    /**
     * Add a HAVING ... LIKE clause to your query.
     */
    public function havingLike($column_name, $value = null)
    {
        return $this->addSimpleHaving($column_name, 'LIKE', $value);
    }

    /**
     * Add where HAVING ... NOT LIKE clause to your query.
     */
    public function havingNotLike($column_name, $value = null)
    {
        return $this->addSimpleHaving($column_name, 'NOT LIKE', $value);
    }

    /**
     * Add a HAVING ... > clause to your query
     */
    public function havingGt($column_name, $value = null)
    {
        return $this->addSimpleHaving($column_name, '>', $value);
    }

    /**
     * Add a HAVING ... < clause to your query
     */
    public function havingLt($column_name, $value = null)
    {
        return $this->addSimpleHaving($column_name, '<', $value);
    }

    /**
     * Add a HAVING ... >= clause to your query
     */
    public function havingGte($column_name, $value = null)
    {
        return $this->addSimpleHaving($column_name, '>=', $value);
    }

    /**
     * Add a HAVING ... <= clause to your query
     */
    public function havingLte($column_name, $value = null)
    {
        return $this->addSimpleHaving($column_name, '<=', $value);
    }

    /**
     * Add a HAVING ... IN clause to your query
     */
    public function havingIn($column_name, $values = null)
    {
        return $this->addHavingPlaceholder($column_name, 'IN', $values);
    }

    /**
     * Add a HAVING ... NOT IN clause to your query
     */
    public function havingNotIn($column_name, $values = null)
    {
        return $this->addHavingPlaceholder($column_name, 'NOT IN', $values);
    }

    /**
     * Add a HAVING column IS NULL clause to your query
     */
    public function havingNull($column_name)
    {
        return $this->addHavingNoValue($column_name, 'IS NULL');
    }

    /**
     * Add a HAVING column IS NOT NULL clause to your query
     */
    public function havingNotNull($column_name)
    {
        return $this->addHavingNoValue($column_name, 'IS NOT NULL');
    }

    /**
     * Add a raw HAVING clause to the query. The clause should
     * contain question mark placeholders, which will be bound
     * to the parameters supplied in the second argument.
     */
    public function havingRaw($clause, $parameters = [])
    {
        return $this->addHaving($clause, $parameters);
    }

    /**
     * Build a SELECT statement based on the clauses that have
     * been passed to this instance by chaining method calls.
     */
    protected function buildSelect()
    {
        // If the query is raw, just set the $this->values to be
        // the raw query parameters and return the raw query
        if ($this->is_raw_query) {
            $this->values = $this->raw_parameters;
            return $this->raw_query;
        }

        // Build and return the full SELECT statement by concatenating
        // the results of calling each separate builder method.
        return $this->joinIfNotEmpty(" ", [
                $this->buildSelectStart(),
                $this->buildJoin(),
                $this->buildWhere(),
                $this->buildGroupBy(),
                $this->buildHaving(),
                $this->buildOrderBy(),
                $this->buildLimit(),
                $this->buildOffset(),
        ]);
    }

    /**
     * Build the start of the SELECT statement
     */
    protected function buildSelectStart()
    {
        $fragment = 'SELECT ';
        $result_columns = join(', ', $this->result_columns);

        if (!is_null($this->limit) &&
            static::$config[$this->connection_name]['limit_clause_style'] === ORM::LIMIT_STYLE_TOP_N) {
            $fragment .= "TOP {$this->limit} ";
        }

        if ($this->distinct) {
            $result_columns = 'DISTINCT ' . $result_columns;
        }

        $fragment .= "{$result_columns} FROM " . $this->quoteIdentifier($this->table_name);

        if (!is_null($this->table_alias)) {
            $fragment .= " " . $this->quoteIdentifier($this->table_alias);
        }
        return $fragment;
    }

    /**
     * Build the JOIN sources
     */
    protected function buildJoin()
    {
        if (count($this->join_sources) === 0) {
            return '';
        }

        return join(" ", $this->join_sources);
    }

    /**
     * Build the WHERE clause(s)
     */
    protected function buildWhere()
    {
        return $this->buildConditions('where');
    }

    /**
     * Build the HAVING clause(s)
     */
    protected function buildHaving()
    {
        return $this->buildConditions('having');
    }

    /**
     * Build GROUP BY
     */
    protected function buildGroupBy()
    {
        if (count($this->group_by) === 0) {
            return '';
        }
        return "GROUP BY " . join(", ", $this->group_by);
    }

    /**
     * Build a WHERE or HAVING clause
     * @param string $type
     * @return string
     */
    protected function buildConditions($type)
    {
        $conditions_class_property_name = "{$type}_conditions";
        // If there are no clauses, return empty string
        if (count($this->$conditions_class_property_name) === 0) {
            return '';
        }

        $conditions = [];
        foreach ($this->$conditions_class_property_name as $condition) {
            $conditions[] = $condition[static::CONDITION_FRAGMENT];
            $this->values = array_merge($this->values, $condition[static::CONDITION_VALUES]);
        }

        return strtoupper($type) . " " . join(" AND ", $conditions);
    }

    /**
     * Build ORDER BY
     */
    protected function buildOrderBy()
    {
        if (count($this->order_by) === 0) {
            return '';
        }
        return "ORDER BY " . join(", ", $this->order_by);
    }

    /**
     * Build LIMIT
     */
    protected function buildLimit()
    {
        $fragment = '';
        if (!is_null($this->limit) &&
            static::$config[$this->connection_name]['limit_clause_style'] == ORM::LIMIT_STYLE_LIMIT) {
            if (static::getDb($this->connection_name)->getAttribute(PDO::ATTR_DRIVER_NAME) == 'firebird') {
                $fragment = 'ROWS';
            } else {
                $fragment = 'LIMIT';
            }
            $fragment .= " {$this->limit}";
        }
        return $fragment;
    }

    /**
     * Build OFFSET
     */
    protected function buildOffset()
    {
        if (!is_null($this->offset)) {
            $clause = 'OFFSET';
            if (static::getDb($this->connection_name)->getAttribute(PDO::ATTR_DRIVER_NAME) == 'firebird') {
                $clause = 'TO';
            }
            return "$clause " . $this->offset;
        }
        return '';
    }

    /**
     * Wrapper around PHP's join function which
     * only adds the pieces if they are not empty.
     */
    protected function joinIfNotEmpty($glue, $pieces)
    {
        $filtered_pieces = [];
        foreach ($pieces as $piece) {
            if (is_string($piece)) {
                $piece = trim($piece);
            }
            if (!empty($piece)) {
                $filtered_pieces[] = $piece;
            }
        }
        return join($glue, $filtered_pieces);
    }

    /**
     * Quote a string that is used as an identifier
     * (table names, column names etc). This method can
     * also deal with dot-separated identifiers eg table.column
     */
    protected function quoteOneIdentifier($identifier)
    {
        $parts = explode('.', $identifier);
        $parts = array_map([$this, 'quoteIdentifierPart'], $parts);
        return join('.', $parts);
    }

    /**
     * Quote a string that is used as an identifier
     * (table names, column names etc) or an array containing
     * multiple identifiers. This method can also deal with
     * dot-separated identifiers eg table.column
     */
    protected function quoteIdentifier($identifier)
    {
        if (is_array($identifier)) {
            $result = array_map([$this, 'quoteOneIdentifier'], $identifier);
            return join(', ', $result);
        } else {
            return $this->quoteOneIdentifier($identifier);
        }
    }

    /**
     * This method performs the actual quoting of a single
     * part of an identifier, using the identifier quote
     * character specified in the config (or autodetected).
     */
    protected function quoteIdentifierPart($part)
    {
        if ($part === '*') {
            return $part;
        }

        $quote_character = static::$config[$this->connection_name]['identifier_quote_character'];
        // double up any identifier quotes to escape them
        return $quote_character .
            str_replace($quote_character, $quote_character . $quote_character, $part
            ) . $quote_character;
    }

    /**
     * Create a cache key for the given query and parameters.
     */
    protected static function createCacheKey($query, $parameters, $table_name = null, $connection_name = self::DEFAULT_CONNECTION)
    {
        if (isset(static::$config[$connection_name]['create_cache_key']) and is_callable(static::$config[$connection_name]['create_cache_key'])) {
            return call_user_func_array(static::$config[$connection_name]['create_cache_key'], [$query, $parameters, $table_name, $connection_name]);
        }
        $parameter_string = join(',', $parameters);
        $key = $query . ':' . $parameter_string;
        return sha1($key);
    }

    /**
     * Check the query cache for the given cache key. If a value
     * is cached for the key, return the value. Otherwise, return false.
     */
    protected static function checkQueryCache($cache_key, $table_name = null, $connection_name = self::DEFAULT_CONNECTION)
    {
        if (isset(static::$config[$connection_name]['check_query_cache']) and is_callable(static::$config[$connection_name]['check_query_cache'])) {
            return call_user_func_array(static::$config[$connection_name]['check_query_cache'], [$cache_key, $table_name, $connection_name]);
        } elseif (isset(static::$query_cache[$connection_name][$cache_key])) {
            return static::$query_cache[$connection_name][$cache_key];
        }
        return false;
    }

    /**
     * Clear the query cache
     */
    public static function clearCache($table_name = null, $connection_name = self::DEFAULT_CONNECTION)
    {
        static::$query_cache = [];
        if (isset(static::$config[$connection_name]['clear_cache']) and is_callable(static::$config[$connection_name]['clear_cache'])) {
            return call_user_func_array(static::$config[$connection_name]['clear_cache'], [$table_name, $connection_name]);
        }
    }

    /**
     * Add the given value to the query cache.
     */
    protected static function cacheQueryResult($cache_key, $value, $table_name = null, $connection_name = self::DEFAULT_CONNECTION)
    {
        if (isset(static::$config[$connection_name]['cache_query_result']) and is_callable(static::$config[$connection_name]['cache_query_result'])) {
            return call_user_func_array(static::$config[$connection_name]['cache_query_result'], [$cache_key, $value, $table_name, $connection_name]);
        } elseif (!isset(static::$query_cache[$connection_name])) {
            static::$query_cache[$connection_name] = [];
        }
        static::$query_cache[$connection_name][$cache_key] = $value;
    }

    /**
     * Execute the SELECT query that has been built up by chaining methods
     * on this class. Return an array of rows as associative arrays.
     */
    protected function run()
    {
        $query = $this->buildSelect();
        $caching_enabled = static::$config[$this->connection_name]['caching'];

        if ($caching_enabled) {
            $cache_key = static::createCacheKey($query, $this->values, $this->table_name, $this->connection_name);
            $cached_result = static::checkQueryCache($cache_key, $this->table_name, $this->connection_name);

            if ($cached_result !== false) {
                return $cached_result;
            }
        }

        static::execute($query, $this->values, $this->connection_name);
        $statement = static::getLastStatement();

        $rows = [];
        while ($row = $statement->fetch(PDO::FETCH_ASSOC)) {
            $rows[] = $row;
        }

        if ($caching_enabled) {
            static::cacheQueryResult($cache_key, $rows, $this->table_name, $this->connection_name);
        }

        // reset Idiorm after executing the query
        $this->values = [];
        $this->result_columns = ['*'];
        $this->using_default_result_columns = true;

        return $rows;
    }

    /**
     * Return the raw data wrapped by this ORM
     * instance as an associative array. Column
     * names may optionally be supplied as arguments,
     * if so, only those keys will be returned.
     */
    public function asArray()
    {
        if (func_num_args() === 0) {
            return $this->data;
        }
        $args = func_get_args();
        return array_intersect_key($this->data, array_flip($args));
    }

    /**
     * Return the value of a property of this object (database row)
     * or null if not present.
     *
     * If a column-names array is passed, it will return a associative array
     * with the value of each column or null if it is not present.
     */
    public function get($key)
    {
        if (is_array($key)) {
            $result = [];
            foreach ($key as $column) {
                $result[$column] = isset($this->data[$column]) ? $this->data[$column] : null;
            }
            return $result;
        } else {
            return isset($this->data[$key]) ? $this->data[$key] : null;
        }
    }

    /**
     * Return the name of the column in the database table which contains
     * the primary key ID of the row.
     */
    protected function getIdColumnName()
    {
        if (!is_null($this->instance_id_column)) {
            return $this->instance_id_column;
        }
        if (isset(static::$config[$this->connection_name]['id_column_overrides'][$this->table_name])) {
            return static::$config[$this->connection_name]['id_column_overrides'][$this->table_name];
        }
        return static::$config[$this->connection_name]['id_column'];
    }

    /**
     * Get the primary key ID of this object.
     */
    public function id($disallow_null = false)
    {
        $id = $this->get($this->getIdColumnName());

        if ($disallow_null) {
            if (is_array($id)) {
                foreach ($id as $id_part) {
                    if ($id_part === null) {
                        throw new Exception('Primary key ID contains null value(s)');
                    }
                }
            } else if ($id === null) {
                throw new Exception('Primary key ID missing from row or is null');
            }
        }

        return $id;
    }

    /**
     * Set a property to a particular value on this object.
     * To set multiple properties at once, pass an associative array
     * as the first parameter and leave out the second parameter.
     * Flags the properties as 'dirty' so they will be saved to the
     * database when save() is called.
     */
    public function set($key, $value = null)
    {
        return $this->setOrmProperty($key, $value);
    }

    /**
     * Set a property to a particular value on this object.
     * To set multiple properties at once, pass an associative array
     * as the first parameter and leave out the second parameter.
     * Flags the properties as 'dirty' so they will be saved to the
     * database when save() is called. 
     * @param string|array $key
     * @param string|null $value
     */
    public function setExpr($key, $value = null)
    {
        return $this->setOrmProperty($key, $value, true);
    }

    /**
     * Set a property on the ORM object.
     * @param string|array $key
     * @param string|null $value
     * @param bool $raw Whether this value should be treated as raw or not
     */
    protected function setOrmProperty($key, $value = null, $expr = false)
    {
        if (!is_array($key)) {
            $key = [$key => $value];
        }
        foreach ($key as $field => $value) {
            $this->data[$field] = $value;
            $this->dirty_fields[$field] = $value;
            if (false === $expr and isset($this->expr_fields[$field])) {
                unset($this->expr_fields[$field]);
            } else if (true === $expr) {
                $this->expr_fields[$field] = true;
            }
        }
        return $this;
    }

    /**
     * Check whether the given field has been changed since this
     * object was saved.
     */
    public function isDirty($key)
    {
        return array_key_exists($key, $this->dirty_fields);
    }

    /**
     * Check whether the model was the result of a call to create() or not
     * @return bool
     */
    public function isNew()
    {
        return $this->is_new;
    }

    /**
     * Save any fields which have been modified on this object
     * to the database.
     */
    public function save()
    {
        $query = [];

        // remove any expression fields as they are already baked into the query
        $values = array_values(array_diff_key($this->dirty_fields, $this->expr_fields));

        if (!$this->is_new) { // UPDATE
            // If there are no dirty values, do nothing
            if (empty($values) && empty($this->expr_fields)) {
                return true;
            }
            $query = $this->buildUpdate();
            $id = $this->id(true);
            if (is_array($id)) {
                $values = array_merge($values, array_values($id));
            } else {
                $values[] = $id;
            }
        } else { // INSERT
            $query = $this->buildInsert();
        }

        $success = static::execute($query, $values, $this->connection_name);
        $caching_auto_clear_enabled = static::$config[$this->connection_name]['caching_auto_clear'];
        if ($caching_auto_clear_enabled) {
            static::clearCache($this->table_name, $this->connection_name);
        }
        // If we've just inserted a new record, set the ID of this object
        if ($this->is_new) {
            $this->is_new = false;
            if ($this->countNullIdColumns() != 0) {
                $db = static::getDb($this->connection_name);
                if ($db->getAttribute(PDO::ATTR_DRIVER_NAME) == 'pgsql') {
                    // it may return several columns if a compound primary
                    // key is used
                    $row = static::getLastStatement()->fetch(PDO::FETCH_ASSOC);
                    foreach ($row as $key => $value) {
                        $this->data[$key] = $value;
                    }
                } else {
                    $column = $this->getIdColumnName();
                    // if the primary key is compound, assign the last inserted id
                    // to the first column
                    if (is_array($column)) {
                        $column = reset($column);
                    }
                    $this->data[$column] = $db->lastInsertId();
                }
            }
        }

        $this->dirty_fields = $this->expr_fields = [];
        return $success;
    }

    /**
     * Add a WHERE clause for every column that belongs to the primary key
     */
    public function addIdColumnConditions(&$query)
    {
        $query[] = "WHERE";
        $keys = is_array($this->getIdColumnName()) ? $this->getIdColumnName() : [$this->getIdColumnName()];
        $first = true;
        foreach ($keys as $key) {
            if ($first) {
                $first = false;
            } else {
                $query[] = "AND";
            }
            $query[] = $this->quoteIdentifier($key);
            $query[] = "= ?";
        }
    }

    /**
     * Build an UPDATE query
     */
    protected function buildUpdate()
    {
        $query = [];
        $query[] = "UPDATE {$this->quoteIdentifier($this->table_name)} SET";

        $field_list = [];
        foreach ($this->dirty_fields as $key => $value) {
            if (!array_key_exists($key, $this->expr_fields)) {
                $value = '?';
            }
            $field_list[] = "{$this->quoteIdentifier($key)} = $value";
        }
        $query[] = join(", ", $field_list);
        $this->addIdColumnConditions($query);
        return join(" ", $query);
    }

    /**
     * Build an INSERT query
     */
    protected function buildInsert()
    {
        $query[] = "INSERT INTO";
        $query[] = $this->quoteIdentifier($this->table_name);
        $field_list = array_map([$this, 'quoteIdentifier'], array_keys($this->dirty_fields));
        $query[] = "(" . join(", ", $field_list) . ")";
        $query[] = "VALUES";

        $placeholders = $this->createPlaceholders($this->dirty_fields);
        $query[] = "({$placeholders})";

        if (static::getDb($this->connection_name)->getAttribute(PDO::ATTR_DRIVER_NAME) == 'pgsql') {
            $query[] = 'RETURNING ' . $this->quoteIdentifier($this->getIdColumnName());
        }

        return join(" ", $query);
    }

    /**
     * Delete this record from the database
     */
    public function delete()
    {
        $query = [
            "DELETE FROM",
            $this->quoteIdentifier($this->table_name)
        ];
        $this->addIdColumnConditions($query);
        return static::execute(join(" ", $query), is_array($this->id(true)) ? array_values($this->id(true)) : [$this->id(true)], $this->connection_name);
    }

    /**
     * Delete many records from the database
     */
    public function deleteMany()
    {
        // Build and return the full DELETE statement by concatenating
        // the results of calling each separate builder method.
        $query = $this->joinIfNotEmpty(" ", [
            "DELETE FROM",
            $this->quoteIdentifier($this->table_name),
            $this->buildWhere(),
        ]);

        return static::execute($query, $this->values, $this->connection_name);
    }

    // --------------------- //
    // ---  ArrayAccess  --- //
    // --------------------- //

    public function offsetExists($key)
    {
        return array_key_exists($key, $this->data);
    }

    public function offsetGet($key)
    {
        return $this->get($key);
    }

    public function offsetSet($key, $value)
    {
        if (is_null($key)) {
            throw new InvalidArgumentException('You must specify a key/array index.');
        }
        $this->set($key, $value);
    }

    public function offsetUnset($key)
    {
        unset($this->data[$key]);
        unset($this->dirty_fields[$key]);
    }

    // --------------------- //
    // --- MAGIC METHODS --- //
    // --------------------- //
    public function __get($key)
    {
        return $this->offsetGet($key);
    }

    public function __set($key, $value)
    {
        $this->offsetSet($key, $value);
    }

    public function __unset($key)
    {
        $this->offsetUnset($key);
    }

    public function __isset($key)
    {
        return $this->offsetExists($key);
    }

    /**
     * Magic method to capture calls to undefined class methods.
     * 
     * @param  string   $method
     * @param  array    $arguments
     * @return ORM
     */
    public function __call($method, $arguments)
    {
        throw new MethodMissingException("Method $method() does not exist in class " . get_class($this));
    }

    /**
     * Magic method to capture calls to undefined static class methods.
     * 
     * @param  string   $method
     * @param  array    $arguments
     * @return ORM
     */
    public static function __callStatic($method, $arguments)
    {
        throw new MethodMissingException("Method $method() does not exist in class Idiorm\ORM");
    }
}
