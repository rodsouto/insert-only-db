<?php

namespace InsertOnlyDb\Tests\Functional;

class AssociationsTest extends FunctionalTestCase {

    public function setUp() {
        parent::setUp();
        $this->createSchema($this->connection->getDoctrineConnection());
    }

    private function createSchema(\Doctrine\DBAL\Connection $connection) {
        $schema = new \Doctrine\DBAL\Schema\Schema();

        $teams = $schema->createTable('teams');
        $teams->addColumn('id', 'integer', array('unsigned' => true, 'autoincrement' => true));
        $teams->addColumn('uuid', 'binary', array('length' => 128));
        $teams->addColumn('created_at', 'datetimetz');
        $teams->addColumn('deleted', 'boolean', array('default' => false));
        $teams->setPrimaryKey(array('id'));

        $players = $schema->createTable('players');
        $players->addColumn('id', 'integer', array('unsigned' => true, 'autoincrement' => true));
        $players->addColumn('uuid', 'binary', array('length' => 128));
        $players->addColumn('created_at', 'datetimetz');
        $players->addColumn('deleted', 'boolean', array('default' => false));
        $players->setPrimaryKey(array('id'));

        $teamsPlayers = $schema->createTable('teams_players');
        $teamsPlayers->addColumn('id', 'integer', array('unsigned' => true, 'autoincrement' => true));
        $teamsPlayers->addColumn('uuid', 'binary', array('length' => 128));
        $teamsPlayers->addColumn('created_at', 'datetimetz');
        $teamsPlayers->addColumn('deleted', 'boolean', array('default' => false));
        $teamsPlayers->addColumn('team_uuid', 'integer', array('unsigned' => true));
        $teamsPlayers->addColumn('player_uuid', 'integer', array('unsigned' => true));
        $teamsPlayers->setPrimaryKey(array('id'));

        foreach($schema->toSql($connection->getDatabasePlatform()) as $query) {
            $connection->executeQuery($query);
        }
    }

    private function getPlayersConfig() {
        return [
            'type' => 'manyToMany',
            'joinTable' => 'teams_players',
            'joinColumns' => ['name' => 'team_uuid', 'referencedColumnName' => 'uuid'],
            'inverseJoinColumns' => ['name' => 'player_uuid', 'referencedColumnName' => 'uuid'],
        ];
    }

    private function loadManyToManyConfig() {
        $team = $this->connection->insert('teams', []);
        $player1 = $this->connection->insert('players', []);
        $player2 = $this->connection->insert('players', []);
        $player3 = $this->connection->insert('players', []);
        $player4 = $this->connection->insert('players', []);

        $this->connection->insert('teams_players', ['team_uuid' => $team, 'player_uuid' => $player1]);
        $this->connection->insert('teams_players', ['team_uuid' => $team, 'player_uuid' => $player2]);
        $this->connection->insert('teams_players', ['team_uuid' => $team, 'player_uuid' => $player3]);
        $this->connection->insert('teams_players', ['team_uuid' => $team, 'player_uuid' => $player4]);

        return ['players' => [$player1, $player2, $player3, $player4], 'team' => $team];
    }

    public function testAssociationManyToMany() {
        $originalUuid = $this->loadManyToManyConfig()['players'];

        $players = $this->connection->fetchAssociation('teams', 'players', $this->getPlayersConfig());

        $this->assertTrue(sizeof($players) == 4);

        $playersUuid = array_column($players, 'uuid');

        sort($playersUuid);
        sort($originalUuid);

        $this->assertEquals($originalUuid, $playersUuid);
    }

    public function testAssociationManyToManyDeleteInverse() {
        $uuidsInfo = $this->loadManyToManyConfig();

        $originalUuid = $uuidsInfo['players'];

        $this->connection->deleteByUuid('players', $originalUuid[0]);
        $this->connection->delete('teams_players', ['team_uuid' => $uuidsInfo['team'], 'player_uuid' => $originalUuid[1]]);
        unset($originalUuid[0], $originalUuid[1]);

        $players = $this->connection->fetchAssociation('teams', 'players', $this->getPlayersConfig());

        $this->assertTrue(sizeof($players) == 2);

        $playersUuid = array_column($players, 'uuid');

        sort($playersUuid);
        sort($originalUuid);

        $this->assertEquals($originalUuid, $playersUuid);
    }

}