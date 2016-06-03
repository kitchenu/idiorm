<?php

use Idiorm\ORM;

class QueryBuilderMssqlTest extends PHPUnit_Framework_TestCase {

    public function setUp() {
        // Enable logging
        ORM::configure('logging', true);

        // Set up the dummy database connection
        $db = new MockMsSqlPDO('sqlite::memory:');
        ORM::setDb($db);
    }

    public function tearDown() {
        ORM::resetConfig();
        ORM::resetDb();
    }

    public function testFindOne() {
        ORM::forTable('widget')->findOne();
        $expected = 'SELECT TOP 1 * FROM "widget"';
        $this->assertEquals($expected, ORM::getLastQuery());
    }

    public function testLimit() {
        ORM::forTable('widget')->limit(5)->findMany();
        $expected = 'SELECT TOP 5 * FROM "widget"';
        $this->assertEquals($expected, ORM::getLastQuery());
    }
    
}

