<?php

namespace InsertOnlyDb;

use Doctrine\DBAL\Connection as DoctrineConnection;
use Rhumsaa\Uuid\Uuid;

class Connection {

    /**
     * @var \Doctrine\DBAL\Connection
     */
    private $connection;

    /**
     * @param DoctrineConnection $connection
     */
    public function __construct(DoctrineConnection $connection) {
        $this->connection = $connection;
    }

    /**
     * @return DoctrineConnection
     */
    public function getDoctrineConnection() {
        return $this->connection;
    }

    /**
     * @param string $tableName
     * @param array $values
     * @return string
     * @throws \RuntimeException
     */
    public function insert($tableName, array $values) {

        if (isset($values['uuid'])) {
            throw new \RuntimeException('UUID must be empty');
        }

        $values['uuid'] = Uuid::uuid4()->toString();

        $this->connection->insert($tableName, $values);

        return $values['uuid'];
    }

    /**
     * @param $tableName
     * @param $uuid
     * @param array $values
     * @return string|false
     */
    public function update($tableName, $uuid, array $values) {

        if (empty($uuid)) {
            throw new \InvalidArgumentException('UUID can\'t be empty');
        }

        $data = $this->fetch($tableName, $uuid);

        if ($data === false) {
            return false;
        }

        unset($data['id']);

        return $this->connection->insert($tableName, array_merge($data, $values));
    }

    /**
     * @param string $tableName
     * @param string $uuid
     * @return array
     */
    public function fetch($tableName, $uuid) {
        $query = $this->connection
                        ->createQueryBuilder()
                        ->select($this->getColumns($tableName))
                        ->from($tableName)
                        ->where('uuid = :uuid')
                        ->orderBy('id', 'DESC')
                        ->setFirstResult(0)
                        ->setMaxResults(1);

        $result = $this->connection->fetchAssoc($query->getSQL(), ['uuid' => $uuid]);

        if (is_array($result)) {
            unset($result['id']);
        }

        return $result;
    }

    public function fetchAll($tableName) {

        $query = $this->connection
            ->createQueryBuilder()
            ->select($this->getColumns($tableName, false))
            ->from($tableName)
            ->where(
                $this->connection
                    ->createQueryBuilder()->expr()->in(
                    'id',
                    $this->connection
                        ->createQueryBuilder()
                        ->select('MAX(id)')
                        ->from($tableName)
                        ->groupBy('uuid')
                        ->getSQL()
                )
            )
            ->orderBy('uuid', 'ASC');

        return $this->connection->fetchAll($query->getSQL());
    }

    /**
     * @param string $tableName
     * @return string
     */
    private function getColumns($tableName, $getId = true) {
        $sm = $this->connection->getSchemaManager();

        $columns = $sm->listTableColumns($tableName);

        if (!$getId) {
            unset($columns['id']);
        }

        return implode(', ', array_keys($columns));
    }

}