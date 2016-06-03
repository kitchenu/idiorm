<?php

use Idiorm\ORM;

class ORMTest extends PHPUnit_Framework_TestCase {

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

    public function testStaticAtrributes() {
        $this->assertEquals('0', ORM::CONDITION_FRAGMENT);
        $this->assertEquals('1', ORM::CONDITION_VALUES);
    }

    public function testForTable() {
        $result = ORM::forTable('test');
        $this->assertInstanceOf('Idiorm\ORM', $result);
    }

    public function testCreate() {
        $model = ORM::forTable('test')->create();
        $this->assertInstanceOf('Idiorm\ORM', $model);
        $this->assertTrue($model->isNew());
    }

    public function testIsNew() {
        $model = ORM::forTable('test')->create();
        $this->assertTrue($model->isNew());

        $model = ORM::forTable('test')->create(['test' => 'test']);
        $this->assertTrue($model->isNew());
    }

    public function testIsDirty() {
        $model = ORM::forTable('test')->create();
        $this->assertFalse($model->isDirty('test'));
        
        $model = ORM::forTable('test')->create(['test' => 'test']);
        $this->assertTrue($model->isDirty('test'));
    }

    public function testArrayAccess() {
        $value = 'test';
        $model = ORM::forTable('test')->create();
        $model['test'] = $value;
        $this->assertTrue(isset($model['test']));
        $this->assertEquals($model['test'], $value);
        unset($model['test']);
        $this->assertFalse(isset($model['test']));
    }

    public function testFindResultSet() {
        $result_set = ORM::forTable('test')->findResultSet();
        $this->assertInstanceOf('Idiorm\ResultSet', $result_set);
        $this->assertSame(count($result_set), 5);
    }

    public function testFindResultSetByDefault() {
        ORM::configure('return_result_sets', true);

        $result_set = ORM::forTable('test')->findMany();
        $this->assertInstanceOf('Idiorm\ResultSet', $result_set);
        $this->assertSame(count($result_set), 5);
        
        ORM::configure('return_result_sets', false);
        
        $result_set = ORM::forTable('test')->findMany();
        $this->assertInternalType('array', $result_set);
        $this->assertSame(count($result_set), 5);
    }

    public function testGetLastPdoStatement() {
        ORM::forTable('widget')->where('name', 'Fred')->findOne();
        $statement = ORM::getLastStatement();
        $this->assertInstanceOf('MockPDOStatement', $statement);
    }

    /**
     * @expectedException Idiorm\MethodMissingException
     */
    public function testInvalidORMFunctionCallShouldCreateException() {
        $orm = ORM::forTable('test');
        $orm->invalidFunctionCall();
    }

    /**
     * @expectedException Idiorm\MethodMissingException
     */
    public function testInvalidResultsSetFunctionCallShouldCreateException() {
        $resultSet = ORM::forTable('test')->findResultSet();
        $resultSet->invalidFunctionCall();
    }

    /**
     * These next two tests are needed because if you have select()ed some fields,
     * but not the primary key, then the primary key is not available for the
     * update/delete query - see issue #203.
     * We need to change the primary key here to something other than `id`
     * becuase MockPDOStatement->fetch() always returns an id.
     */
    public function testUpdateNullPrimaryKey() {
        try {
            $widget = ORM::forTable('widget')
                ->useIdColumn('primary')
                ->select('foo')
                ->where('primary', 1)
                ->findOne()
            ;

            $widget->foo = 'bar';
            $widget->save();

            throw new Exception('Test did not throw expected exception');
        } catch (Exception $e) {
            $this->assertEquals($e->getMessage(), 'Primary key ID missing from row or is null');
        }
    }

    public function testDeleteNullPrimaryKey() {
        try {
            $widget = ORM::forTable('widget')
                ->useIdColumn('primary')
                ->select('foo')
                ->where('primary', 1)
                ->findOne()
            ;

            $widget->delete();

            throw new Exception('Test did not throw expected exception');
        } catch (Exception $e) {
            $this->assertEquals($e->getMessage(), 'Primary key ID missing from row or is null');
        }
    }

    public function testNullPrimaryKey() {
        try {
            $widget = ORM::forTable('widget')
                ->useIdColumn('primary')
                ->select('foo')
                ->where('primary', 1)
                ->findOne()
            ;

            $widget->id(true);

            throw new Exception('Test did not throw expected exception');
        } catch (Exception $e) {
            $this->assertEquals($e->getMessage(), 'Primary key ID missing from row or is null');
        }
    }

    public function testNullPrimaryKeyPart() {
        try {
            $widget = ORM::forTable('widget')
                ->useIdColumn(['id', 'primary'])
                ->select('foo')
                ->where('id', 1)
                ->where('primary', 1)
                ->findOne()
            ;

            $widget->id(true);

            throw new Exception('Test did not throw expected exception');
        } catch (Exception $e) {
            $this->assertEquals($e->getMessage(), 'Primary key ID contains null value(s)');
        }
    }
}