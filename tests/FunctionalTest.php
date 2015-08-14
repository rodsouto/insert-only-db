<?php

namespace InsertOnlyDb\Tests;

use InsertOnlyDb\Connection;

class Functional extends \PHPUnit_Framework_TestCase {

    /** @var Connection */
    private $connection;

    private $tableName = 'test_table';

    public function setUp() {
        $doctrineConnection = $this->getDoctrineConnection();
        $this->connection = new Connection($doctrineConnection);
        $this->createSchema($doctrineConnection);
    }

    public function tearDown() {
        $this->connection->getDoctrineConnection()->close();
    }

    private function getDoctrineConnection() {
        $config = new \Doctrine\DBAL\Configuration();

        $connectionParams = array(
            'memory' => true,
            'driver' => 'pdo_sqlite',
        );
        $conn = \Doctrine\DBAL\DriverManager::getConnection($connectionParams, $config);

        return $conn;
    }

    private function createSchema(\Doctrine\DBAL\Connection $connection) {
        $schema = new \Doctrine\DBAL\Schema\Schema();

        $myTable = $schema->createTable($this->tableName);

        $myTable->addColumn('id', 'integer', array('unsigned' => true, 'autoincrement' => true));
        $myTable->addColumn('uuid', 'binary', array('length' => 128));

        $myTable->addColumn('field1', 'string', array('length' => 32));
        $myTable->addColumn('field2', 'string', array('length' => 32));

        $myTable->setPrimaryKey(array('id'));

        foreach($schema->toSql($connection->getDatabasePlatform()) as $query) {
            $connection->executeQuery($query);
        }
    }

    public function testInsert() {
        $insert = ['field1' => 'value1', 'field2' => 'value2'];
        $uuid = $this->connection->insert($this->tableName, $insert);

        $insert['uuid'] = $uuid;

        $result = $this->connection->fetch($this->tableName, $uuid);

        $this->assertEquals($insert, $result);
    }

    public function testUpdate() {
        $insert = ['field1' => 'value1', 'field2' => 'value2'];
        $uuid = $this->connection->insert($this->tableName, $insert);

        $update = ['field2' => 'value3'];
        $this->connection->update($this->tableName, $uuid, $update);

        $result = $this->connection->fetch($this->tableName, $uuid);

        $this->assertEquals(array_merge($insert, $update)+['uuid' => $uuid], $result);
    }

    public function testMultipleUpdates() {
        $insert = ['field1' => 'value1', 'field2' => 'value2'];
        $uuid = $this->connection->insert($this->tableName, $insert);

        foreach([2, 3, 4] as $value) {
            $this->connection->update($this->tableName, $uuid, ['field2' => $value]);

            $result = $this->connection->fetch($this->tableName, $uuid);

            $this->assertEquals($value, $result['field2']);
        }

    }

    public function testIdIsNotInResult() {
        $insert = ['field1' => 'value1', 'field2' => 'value2'];
        $uuid = $this->connection->insert($this->tableName, $insert);

        $result = $this->connection->fetch($this->tableName, $uuid);

        $this->assertArrayNotHasKey('id', $result);
    }

    public function testNoResultsReturnsFalse() {
        $this->assertFalse($this->connection->fetch($this->tableName, 'fail'));
    }

    public function testUuidOnInsertThrowsException() {
        $this->setExpectedException('RuntimeException');

        $this->connection->insert($this->tableName, ['uuid' => 'fail']);
    }

}