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
     * @param string $tableName
     * @param string $uuid
     * @param array $values
     * @return string|false
     */
    public function update($tableName, $uuid, array $values) {

        if (empty($uuid)) {
            throw new \InvalidArgumentException('UUID can\'t be empty');
        }

        $data = $this->fetchByUuid($tableName, $uuid);

        if ($data === false) {
            return false;
        }

        unset($data['id']);

        return $this->connection->insert($tableName, array_merge($data, $values));
    }

    /**
     * @param string $tableName
     * @param array $where
     * @return int|false
     */
    public function delete($tableName, array $where) {
        $data = $this->fetch($tableName, $where);

        if (empty($data)) {
            return false;
        }

        $data['deleted'] = 1;

        return $this->connection->insert($tableName, $data);
    }

    /**
     * @param string $tableName
     * @param string $uuid
     * @return int|false
     */
    public function deleteByUuid($tableName, $uuid) {
        return $this->delete($tableName, ['uuid' => $uuid]);
    }

    /**
     * @param string $tableName
     * @param array $where
     * @return array|false
     */
    public function fetch($tableName, array $where) {
        $query = $this->connection
                        ->createQueryBuilder()
                        ->select($this->getColumns($tableName))
                        ->from($tableName)
                        ->orderBy('id', 'DESC')
                        ->setFirstResult(0)
                        ->setMaxResults(1);

        foreach($where as $field => $value) {
            $query->andWhere($field.' = ?');
        }

        $result = $this->connection->fetchAssoc($query->getSQL(), array_values($where));

        if (is_array($result)) {
            unset($result['id']);
        }

        if ($result['deleted'] == 1) {
            return false;
        }

        return $result;
    }

    /**
     * @param string $tableName
     * @param string $uuid
     * @return array|false
     */
    public function fetchByUuid($tableName, $uuid) {
        return $this->fetch($tableName, ['uuid' => $uuid]);
    }

    /**
     * @param string $tableName
     * @return array
     */
    public function fetchAll($tableName) {

        $query = $this->connection
            ->createQueryBuilder()
            ->select($this->getColumns($tableName, 't', false))
            ->from($tableName, 't')
            ->where(
                $this->getWhereMaxIdInSql('t.id', $tableName, 'uuid')
            )
            ->andWhere('t.deleted = 0')
            ->orderBy('t.uuid', 'ASC');

        return $this->connection->fetchAll($query->getSQL());
    }

    public function fetchAssociation($owningTable, $inverseTable, array $config) {

        if (empty($config['type'])) {
            throw new \InvalidArgumentException('Asociation type can\'t be empty');
        }

        if ($config['type'] != 'manyToMany') {
            throw new \InvalidArgumentException('Only ManyToMany associations are supported');
        }

        $inverseJoinColumns = $config['inverseJoinColumns'];
        $joinColumns = $config['joinColumns'];

        $query = $this->connection
            ->createQueryBuilder()
            ->select($this->getColumns($inverseTable, 'i'))
            ->from($inverseTable, 'i')
            ->innerJoin(
                'i',
                $config['joinTable'],
                'j',
                sprintf('i.%s = j.%s', $inverseJoinColumns['referencedColumnName'], $inverseJoinColumns['name'])
            )
            ->innerJoin(
                'j',
                $owningTable,
                'o',
                sprintf('j.%s = o.%s', $joinColumns['name'], $joinColumns['referencedColumnName'])
            )
            ->andWhere(
                $this->getWhereMaxIdInSql('i.id', $inverseTable, 'uuid')
            )
            ->andWhere(
                $this->getWhereMaxIdInSql('j.id', $config['joinTable'], $inverseJoinColumns['name'])
            )
            ->andWhere('j.deleted = 0')
            ->andWhere('i.deleted = 0')
            ->orderBy('i.uuid', 'ASC');

        return $this->connection->fetchAll($query->getSQL());
    }

    private function getWhereMaxIdInSql($fieldIn, $tableName, $groupBy) {
        return $this->connection
            ->createQueryBuilder()
            ->expr()
            ->in(
                $fieldIn,
                $this->connection
                    ->createQueryBuilder()
                    ->select('MAX(id)')
                    ->from($tableName)
                    ->groupBy($groupBy)
                    ->getSQL()
            );
    }

    /**
     * @param string $tableName
     * @return string
     */
    private function getColumns($tableName, $tableAlias = '', $getId = true) {
        $sm = $this->connection->getSchemaManager();

        $columns = $sm->listTableColumns($tableName);

        if (!$getId) {
            unset($columns['id']);
        }

        $columns = array_keys($columns);

        if ($tableAlias) {
            $columns = array_map(function($tableName) use ($tableAlias) {return $tableAlias.'.'.$tableName;}, $columns);
        }

        return implode(', ', $columns);
    }

}