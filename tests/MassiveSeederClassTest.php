<?php

use Elminson\MassiveSeeder\MassiveSeederCommand;
use Illuminate\Support\Facades\Config;
use Illuminate\Support\Facades\DB;

it('tests the MassiveSeederCommand', function () {
    // Mock the DB config
    Config::set('database.connections.mysql', [
        'driver' => 'mysql',
        'host' => '127.0.0.1',
        'port' => '3306',
        'database' => 'database',
        'username' => 'root',
        'password' => '',
        // ... other config values
    ]);

    // Mock the DB facade to assert that insert method was called
    DB::shouldReceive('connection->getSchemaBuilder->getColumnListing')
        ->andReturn(['name', 'email', 'address']); // replace with your columns

    DB::shouldReceive('connection->table->insert')
        ->once();

    // Mock the user's input for the ask method
    $this->artisan(MassiveSeederCommand::class)
        ->expectsQuestion('What connection do you want to use?', 'mysql')
        ->expectsQuestion('What table do you want to seed?', 'users')
        ->assertExitCode(0);

    // Assert the command output
    $this->expectsOutput('Data seeded successfully!');
});
