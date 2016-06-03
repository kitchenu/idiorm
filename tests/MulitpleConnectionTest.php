<?php

use Idiorm\ORM;

class MultipleConnectionTest extends PHPUnit_Framework_TestCase {

    const ALTERNATE = 'alternate'; // Used as name of alternate connection

    public function setUp() {
        // Set up the dummy database connections
        ORM::setDb(new MockPDO('sqlite::memory:'));
        ORM::setDb(new MockDifferentPDO('sqlite::memory:'), self::ALTERNATE);

        // Enable logging
        ORM::configure('logging', true);
        ORM::configure('logging', true, self::ALTERNATE);
    }

    public function tearDown() {
        ORM::resetConfig();
        ORM::resetDb();
    }

    public function testMultiplePdoConnections() {
        $this->assertInstanceOf('MockPDO', ORM::getDb());
        $this->assertInstanceOf('MockPDO', ORM::getDb(ORM::DEFAULT_CONNECTION));
        $this->assertInstanceOf('MockDifferentPDO', ORM::getDb(self::ALTERNATE));
    }

    public function testRawExecuteOverAlternateConnection() {
        $expected = "SELECT * FROM `foo`";
        ORM::rawExecute("SELECT * FROM `foo`", [], self::ALTERNATE);

        $this->assertEquals($expected, ORM::getLastQuery(self::ALTERNATE));
    }

    public function testFindOneOverDifferentConnections() {
        ORM::forTable('widget')->findOne();
        $statementOne = ORM::getLastStatement();
        $this->assertInstanceOf('MockPDOStatement', $statementOne);

        ORM::forTable('person', self::ALTERNATE)->findOne();
        $statementOne = ORM::getLastStatement(); // get_statement is *not* per connection
        $this->assertInstanceOf('MockDifferentPDOStatement', $statementOne);

        $expected = "SELECT * FROM `widget` LIMIT 1";
        $this->assertNotEquals($expected, ORM::getLastQuery()); // Because getLastQuery() is across *all* connections
        $this->assertEquals($expected, ORM::getLastQuery(ORM::DEFAULT_CONNECTION));

        $expectedToo = "SELECT * FROM `person` LIMIT 1";
        $this->assertEquals($expectedToo, ORM::getLastQuery(self::ALTERNATE));
    }

}