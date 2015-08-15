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

        $myTable->addColumn('deleted', 'boolean', array('default' => false));

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
        $insert['deleted'] = 0;

        $result = $this->connection->fetch($this->tableName, $uuid);

        $this->assertEquals($insert, $result);
    }

    public function testUpdate() {
        $insert = ['field1' => 'value1', 'field2' => 'value2'];
        $uuid = $this->connection->insert($this->tableName, $insert);

        $update = ['field2' => 'value3'];
        $this->connection->update($this->tableName, $uuid, $update);

        $result = $this->connection->fetch($this->tableName, $uuid);

        $this->assertEquals(array_merge($insert, $update)+['uuid' => $uuid, 'deleted' => 0], $result);
    }

    public function testUpdateWithoutUuidThrowsException() {
        $this->setExpectedException('InvalidArgumentException');
        $this->connection->update($this->tableName, null, ['field1' => 2]);
    }

    public function testUpdateInexistentUuid() {
        $result = $this->connection->update($this->tableName, 'invalid_uuid', ['field' => 2]);
        $this->assertFalse($result);
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

    public function testFetchNoResultsReturnsFalse() {
        $this->assertFalse($this->connection->fetch($this->tableName, 'fail'));
    }

    public function testUuidOnInsertThrowsException() {
        $this->setExpectedException('RuntimeException');

        $this->connection->insert($this->tableName, ['uuid' => 'fail']);
    }

    public function testFetchAllReturnsLastVersionOrderedByUuidAsc() {
        $uuid1 = $this->connection->insert($this->tableName, ['field1' => 1, 'field2' => 5]);
        $uuid2 = $this->connection->insert($this->tableName, ['field1' => 2, 'field2' => 6]);
        $uuid3 = $this->connection->insert($this->tableName, ['field1' => 3, 'field2' => 7]);
        $uuid4 = $this->connection->insert($this->tableName, ['field1' => 4, 'field2' => 8]);

        $this->connection->update($this->tableName, $uuid1, ['field2' => 9]);
        $this->connection->update($this->tableName, $uuid2, ['field2' => 10]);
        $this->connection->update($this->tableName, $uuid3, ['field2' => 11]);
        $this->connection->update($this->tableName, $uuid4, ['field2' => 12]);

        $results = $this->connection->fetchAll($this->tableName);

        $uuids = [$uuid1, $uuid2, $uuid3, $uuid4];

        sort($uuids);

        foreach($uuids as $index => $uuid) {
            $this->assertEquals($uuid, $results[$index]['uuid']);

            $this->assertArrayNotHasKey('id', $results[$index]);

            switch($uuid) {
                case $uuid1:
                    $this->assertEquals(9, $results[$index]['field2']);
                break;
                case $uuid2:
                    $this->assertEquals(10, $results[$index]['field2']);
                break;
                case $uuid3:
                    $this->assertEquals(11, $results[$index]['field2']);
                break;
                case $uuid4:
                    $this->assertEquals(12, $results[$index]['field2']);
                break;
                default:
                    throw new \RuntimeException('Woops');
                break;
            }

        }
    }

    public function testDeleteAndFetch() {
        $uuid = $this->connection->insert($this->tableName, ['field1' => 2, 'field2' => 3]);

        $this->assertNotEmpty($this->connection->fetch($this->tableName, $uuid));

        $this->connection->delete($this->tableName, $uuid);

        $this->assertFalse($this->connection->fetch($this->tableName, $uuid));
    }

    public function testDeleteAndFetchAll() {
        $uuid1 = $this->connection->insert($this->tableName, ['field1' => 2, 'field2' => 3]);
        $uuid2 = $this->connection->insert($this->tableName, ['field1' => 4, 'field2' => 5]);

        $this->assertTrue(sizeof($this->connection->fetchAll($this->tableName)) == 2);

        $this->connection->delete($this->tableName, $uuid1);

        $result = $this->connection->fetchAll($this->tableName);

        $this->assertTrue(sizeof($result) == 1);
        $this->assertEquals($uuid2, $result[0]['uuid']);
    }

}