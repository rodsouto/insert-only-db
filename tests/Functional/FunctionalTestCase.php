<?php

namespace InsertOnlyDb\Tests\Functional;

use InsertOnlyDb\Connection;

class FunctionalTestCase extends \PHPUnit_Framework_TestCase {

    /** @var Connection */
    protected $connection;

    public function setUp() {
        $this->connection = new Connection($this->getDoctrineConnection());
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

}