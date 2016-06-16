<?php

use Idiorm\ORM;
use Idiorm\ResultSet;

class ResultSetTest extends PHPUnit_Framework_TestCase {

    public function setUp() {
        // Enable logging
        ORM::configure('logging', true);

        // Set up the dummy database connection
        $db = new MockPDO('sqlite::memory:');
        ORM::setDb($db);
    }

    public function tearDown() {
        ORM::resetConfig();
        ORM::resetDb();
    }

    public function testGet() {
        $ResultSet = new ResultSet();
        $this->assertInternalType('array', $ResultSet->getResults());
    }

    public function testConstructor() {
        $result_set = ['item' => new stdClass];
        $ResultSet = new ResultSet($result_set);
        $this->assertSame($ResultSet->getResults(), $result_set);
    }

    public function testSetResultsAndGetResults() {
        $result_set = ['item' => new stdClass];
        $ResultSet = new ResultSet();
        $ResultSet->setResults($result_set);
        $this->assertSame($ResultSet->getResults(), $result_set);
    }

    public function testAsArray() {
        $result_set = ['item' => new stdClass];
        $ResultSet = new ResultSet();
        $ResultSet->setResults($result_set);
        $this->assertSame($ResultSet->asArray(), $result_set);
    }

    public function testAsJson() {
        $result_set = ['item' => new stdClass];
        $ResultSet = new ResultSet();
        $ResultSet->setResults($result_set);
        $this->assertSame($ResultSet->asJson(), '{"item":{}}');
    }

    public function testCount() {
        $result_set = ['item' => new stdClass];
        $ResultSet = new ResultSet($result_set);
        $this->assertSame($ResultSet->count(), 1);
        $this->assertSame(count($ResultSet), 1);
    }

    public function testGetIterator() {
        $result_set = ['item' => new stdClass];
        $ResultSet = new ResultSet($result_set);
        $this->assertInstanceOf('ArrayIterator', $ResultSet->getIterator());
    }

    public function testForeach() {
        $result_set = ['item' => new stdClass];
        $ResultSet = new ResultSet($result_set);
        $return_array = array();
        foreach ($ResultSet as $key => $record) {
            $return_array[$key] = $record;
        }
        $this->assertSame($result_set, $return_array);
    }

    public function testCallingMethods() {
        $result_set = ['item' => ORM::forTable('test'), 'item2' => ORM::forTable('test')];
        $ResultSet = new ResultSet($result_set);
        $ResultSet->set('field', 'value')->set('field2', 'value');

        foreach ($ResultSet as $record) {
            $this->assertTrue(isset($record->field));
            $this->assertSame($record->field, 'value');

            $this->assertTrue(isset($record->field2));
            $this->assertSame($record->field2, 'value');
        }
    }

    public function testOffset() {
        $ResultSet = new ResultSet();
        $ResultSet->offsetSet('item', new stdClass);
        $this->assertTrue($ResultSet->offsetExists('item'));
        $this->assertInstanceOf('stdClass', $ResultSet->offsetGet('item'));

        $ResultSet->offsetUnset('item');
        $this->assertFalse($ResultSet->offsetExists('item'));
    }

    public function testSelialize() {
        $result_set = ['item' => new stdClass];
        $ResultSet = new ResultSet($result_set);
        $serial = $ResultSet->serialize();
        $this->assertEquals($result_set, $ResultSet->unserialize($serial));
    }
}
