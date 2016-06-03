<?php

use Idiorm\ORM;

class ConfigTest extends PHPUnit_Framework_TestCase {

    public function setUp() {
        // Enable logging
        ORM::configure('logging', true);

        // Set up the dummy database connection
        $db = new MockPDO('sqlite::memory:');
        ORM::setDb($db);

        ORM::configure('id_column', 'primary_key');
    }

    public function tearDown() {
        ORM::resetConfig();
        ORM::resetDb();
    }

    protected function setUpIdColumnOverrides() {
        ORM::configure('id_column_overrides', [
            'widget' => 'widget_id',
            'widget_handle' => 'widget_handle_id',
        ]);
    }

    protected function tearDownIdColumnOverrides() {
        ORM::configure('id_column_overrides', []);
    }

    public function testSettingIdColumn() {
        ORM::forTable('widget')->findOne(5);
        $expected = "SELECT * FROM `widget` WHERE `primary_key` = '5' LIMIT 1";
        $this->assertEquals($expected, ORM::getLastQuery());
    }

    public function testSettingIdColumnOverridesOne() {
        $this->setUpIdColumnOverrides();

        ORM::forTable('widget')->findOne(5);
        $expected = "SELECT * FROM `widget` WHERE `widget_id` = '5' LIMIT 1";
        $this->assertEquals($expected, ORM::getLastQuery());

        $this->tearDownIdColumnOverrides();
    }

    public function testSettingIdColumnOverridesTwo() {
        $this->setUpIdColumnOverrides();

        ORM::forTable('widget_handle')->findOne(5);
        $expected = "SELECT * FROM `widget_handle` WHERE `widget_handle_id` = '5' LIMIT 1";
        $this->assertEquals($expected, ORM::getLastQuery());

        $this->tearDownIdColumnOverrides();
    }

    public function testSettingIdColumnOverridesThree() {
        $this->setUpIdColumnOverrides();

        ORM::forTable('widget_nozzle')->findOne(5);
        $expected = "SELECT * FROM `widget_nozzle` WHERE `primary_key` = '5' LIMIT 1";
        $this->assertEquals($expected, ORM::getLastQuery());

        $this->tearDownIdColumnOverrides();
    }

    public function testInstanceIdColumnOne() {
        $this->setUpIdColumnOverrides();

        ORM::forTable('widget')->useIdColumn('new_id')->findOne(5);
        $expected = "SELECT * FROM `widget` WHERE `new_id` = '5' LIMIT 1";
        $this->assertEquals($expected, ORM::getLastQuery());

        $this->tearDownIdColumnOverrides();
    }

    public function testInstanceIdColumnTwo() {
        $this->setUpIdColumnOverrides();

        ORM::forTable('widget_handle')->useIdColumn('new_id')->findOne(5);
        $expected = "SELECT * FROM `widget_handle` WHERE `new_id` = '5' LIMIT 1";
        $this->assertEquals($expected, ORM::getLastQuery());

        $this->tearDownIdColumnOverrides();
    }

    public function testInstanceIdColumnThree() {
        $this->setUpIdColumnOverrides();

        ORM::forTable('widget_nozzle')->useIdColumn('new_id')->findOne(5);
        $expected = "SELECT * FROM `widget_nozzle` WHERE `new_id` = '5' LIMIT 1";
        $this->assertEquals($expected, ORM::getLastQuery());

        $this->tearDownIdColumnOverrides();
    }

    public function testGetConfig() {
        $this->assertTrue(ORM::getConfig('logging'));
        ORM::configure('logging', false);
        $this->assertFalse(ORM::getConfig('logging'));
        ORM::configure('logging', true);
    }

    public function testGetConfigArray() {
        $expected = [
            'connection_string' => 'sqlite::memory:',
            'id_column' => 'primary_key',
            'id_column_overrides' => [],
            'error_mode' => PDO::ERRMODE_EXCEPTION,
            'username' => null,
            'password' => null,
            'driver_options' => null,
            'identifier_quote_character' => '`',
            'logging' => true,
            'logger' => null,
            'caching' => false,
            'caching_auto_clear' => false,
            'return_result_sets' => false,
            'limit_clause_style' => 'limit',
        ];
        $this->assertEquals($expected, ORM::getConfig());
    }

    public function testLoggerCallback() {
        ORM::configure('logger', function($log_string) {
            return $log_string;
        });
        $function = ORM::getConfig('logger');
        $this->assertTrue(is_callable($function));

        $log_string = "UPDATE `widget` SET `added` = NOW() WHERE `id` = '1'";
        $this->assertEquals($log_string, $function($log_string));

        ORM::configure('logger', null);
    }

}
