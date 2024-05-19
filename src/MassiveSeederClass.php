<?php

namespace Elminson\MassiveSeeder;

use Faker\Factory as Faker;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Spatie\Fork\Fork;

use function Laravel\Prompts\select;
use function Laravel\Prompts\text;

class MassiveSeederCommand extends Command
{
    const SUPPORTED_DRIVERS = ['mysql', 'sqlite', 'pgsql'];

    protected $signature = 'db:massive-seeder';

    protected $description = 'Seed a database table with massive data';

    private $values = [];

    private int $numTasks = 50;

    private \Faker\Generator $faker;

    private string|int $table;

    private string|int $connection;

    public function __construct()
    {
        parent::__construct();
        $this->faker = Faker::create();
    }

    public function handle()
    {

        if (app()->environment() !== 'production') {
            $this->error('You are running this command in the production environment.');

            if (! $this->confirm('Do you wish to continue?')) {
                $this->info('Exiting...');

                return 0;
            }
        }

        $connections = array_keys(config('database.connections'));
        $connections = array_filter($connections, function ($connection) {
            return in_array(config("database.connections.$connection.driver"), self::SUPPORTED_DRIVERS);
        });

        $this->connection = select(
            'Select the connection to use.',
            array_values($connections)
        );

        $this->info('You selected: '.$this->connection);
        if (! in_array(config("database.connections.$this->connection.driver"), self::SUPPORTED_DRIVERS)) {
            $this->error($this->connection.' is not supported for this command.');

            return 1;
        }

        try {
            match (config("database.connections.$this->connection.driver")) {
                'mysql' => $tables = $this->allTablesMySQL($this->connection),
                'sqlite' => $tables = $this->allTablesSqlite($this->connection),
                'pgsql' => $tables = $this->allTablesPgsql($this->connection),
            };
        } catch (\Exception $e) {
            $this->error('An error occurred while fetching the tables: '.$e->getMessage());

            return 1;
        }

        if (empty($tables)) {
            $this->error('No tables found in the selected connection.');

            return 1;
        }

        $this->table = select(
            'Select the table to seed.',
            $tables
        );

        $this->info('You selected: '.$this->table.' table');

        $batchSize = 1000;
        $totalRecords = text(
            label: 'How many records do you want to insert? (max: 1,000,000)',
            validate: ['name' => 'required|max:1000000|integer'],
        );

        $start = microtime(true);

        $this->numTasks = 50;
        $iterations = intdiv($totalRecords, $batchSize);
        $bar = $this->output->createProgressBar($iterations);
        $tasks = [];

        for ($i = 0; $i < $this->numTasks; $i++) {
            $tasks[$i] = fn () => $this->processSeed($totalRecords / $this->numTasks, $batchSize / $this->numTasks);
        }
        $bar->start();
        $results = Fork::new()
            ->after(
                child: fn () => DB::connection($this->connection)->disconnect(),
                parent: fn () => $bar->advance(),
            )
            ->before(fn () => DB::connection($this->connection)->reconnect())
            ->concurrent($this->numTasks)
            ->run(...$tasks);

        $end = microtime(true);

        $this->line('');
        // Display the memory usage
        $this->info($this->displayMemoryUsage());
        $this->info('Total Records Inserted: '.number_format($totalRecords, 0, '', ','));
        $this->info('Data seeded successfully!');

        $executionTime = $end - $start;

        $this->info('handle method execution time: '.round($executionTime, 2).' seconds');

        return 0;

    }

    public function processSeed($totalRecords, $batchSize)
    {

        $start = microtime(true);
        $iterations = intdiv($totalRecords, $batchSize);
        $columnsAndTypes = $this->getColumnsAndTypes($this->connection, $this->table);

        for ($i = 0; $i < $iterations; $i++) {
            foreach ($this->generateDataBatch($batchSize, $columnsAndTypes) as $data) {
                DB::connection($this->connection)->table($this->table)->insert($data);
            }
        }

        // $bar->finish();
        return microtime(true) - $start;

    }

    public static function allTablesMySQL($connection = null)
    {
        $tablesData = collect(DB::connection($connection)->select('show tables'));
        $tables = [];
        foreach ($tablesData as $table) {
            $tables[] = $table->tablename;
        }

        return $tables;
    }

    public static function allTablesSqlite($connection = null)
    {
        $tablesData = collect(DB::connection($connection)->select("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"));
        $tables = [];
        foreach ($tablesData as $table) {
            $tables[] = $table->tablename;
        }

        return $tables;
    }

    public static function allTablesPgsql($connection = null)
    {
        $tablesData = collect(DB::connection($connection)->select("SELECT * FROM pg_catalog.pg_tables WHERE schemaname='public'"));
        $tables = [];
        foreach ($tablesData as $table) {
            $tables[] = $table->tablename;
        }

        return $tables;
    }

    private function generateDataBatch($batchSize, $columns)
    {
        for ($j = 0; $j < $batchSize; $j++) {
            $data = [];

            foreach ($columns as $column => $type) {
                switch ($type) {
                    case 'name':
                        $data[$column] = $this->faker->name;
                        break;
                    case 'email':
                        $data[$column] = $this->faker->email;
                        break;
                    case 'address':
                        $data[$column] = $this->faker->address;
                        break;
                    case 'integer':
                    case 'int4':
                    case 'float4':
                        $data[$column] = $this->faker->randomNumber();
                        break;
                    case 'timestamp':
                        $data[$column] = $this->faker->dateTimeThisYear()->format('Y-m-d H:i:s');
                        break;
                    case 'varchar':
                        $data[$column] = $this->faker->word;
                        break;
                    case 'text':
                        $data[$column] = $this->faker->text;
                        break;
                    case 'bool':
                        $data[$column] = $this->faker->boolean;
                        break;
                    default:
                        $data[$column] = $this->faker->word;
                }
            }

            yield $data;
        }
    }

    public function getColumnsAndTypes($connection, $table)
    {
        $columns = DB::connection($connection)->getSchemaBuilder()->getColumnListing($table);
        $columnsWithTypes = [];

        foreach ($columns as $column) {
            $type = DB::connection($connection)->getSchemaBuilder()->getColumnType($table, $column);
            $columnsWithTypes[$column] = $type;
        }

        return $columnsWithTypes;
    }

    private function displayMemoryUsage()
    {
        $memoryUsage = memory_get_usage(true);
        if ($memoryUsage < 1024) {
            return 'Memory usage: '.$memoryUsage.' bytes';
        } elseif ($memoryUsage < 1048576) {
            return 'Memory usage: '.round($memoryUsage / 1024, 2).' KB';
        } else {
            return 'Memory usage: '.round($memoryUsage / 1048576, 2).' MB';
        }
    }
}
