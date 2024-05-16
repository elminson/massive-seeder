<?php

namespace Elminson\MassiveSeeder;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Faker\Factory as Faker;
use Spatie\Async\Pool;
use Symfony\Component\Console\Helper\ProgressBar;
use Symfony\Component\Console\Output\ConsoleOutput;
use function Laravel\Prompts\select;
use function Laravel\Prompts\text;
use Spatie\Fork\Fork;

class MassiveSeederCommand extends Command
{
	const SUPPORTED_DRIVERS = ['mysql', 'sqlite', 'pgsql'];

	protected $signature = 'db:massive-seeder';

	protected $description = 'Seed a database table with massive data';
	private $values = [];
	private $numTasks = 50;

	public function handle()
	{

		if (app()->environment() !== 'production') {
			$this->error('You are running this command in the production environment.');

			if (!$this->confirm('Do you wish to continue?')) {
				$this->info('Exiting...');
				return 0;
			}
		}

		$connections = array_keys(config('database.connections'));
		$connections = array_filter($connections, function ($connection) {
			return in_array(config("database.connections.$connection.driver"), self::SUPPORTED_DRIVERS);
		});

		$connection = select(
			'Select the connection to use.',
			array_values($connections)
		);

		$this->info('You selected: ' . $connection);
		if (!in_array(config("database.connections.$connection.driver"), self::SUPPORTED_DRIVERS)) {
			$this->error($connection . ' is not supported for this command.');
			return 1;
		}

		try {
			match (config("database.connections.$connection.driver")) {
				'mysql' => $tables = $this->allTablesMySQL($connection),
				'sqlite' => $tables = $this->allTablesSqlite($connection),
				'pgsql' => $tables = $this->allTablesPgsql($connection),
			};
		} catch (\Exception $e) {
			$this->error('An error occurred while fetching the tables: ' . $e->getMessage());
			return 1;
		}

		if (empty($tables)) {
			$this->error('No tables found in the selected connection.');
			return 1;
		}

		$table = select(
			'Select the table to seed.',
			$tables
		);

		$this->info('You selected: ' . $table . ' table');

		$batchSize = 1000;
		$totalRecords = text(
			label: 'How many records do you want to insert? (max: 1,000,000)',
			validate: ['name' => 'required|max:1000000|integer'],
		);

		$start = microtime(true);

		$iterations = intdiv($totalRecords, $batchSize);
		$bar = $this->output->createProgressBar($iterations);
		$tasks = [];

		for ($i = 0; $i < $this->numTasks; $i++) {
			$tasks[$i] = fn () => $this->processSeed($totalRecords/$this->numTasks, $batchSize/$this->numTasks, $connection, $table);
		}
		$bar->start();
		$results = Fork::new()
					   ->after(
						   child: fn () => DB::connection($connection)->disconnect(),
						   parent: fn (int $iterations) =>
						   $bar->advance($iterations),
					   )
					   ->before(fn() => DB::connection($connection)->reconnect())
					   ->concurrent($this->numTasks)
					   ->run(...$tasks);

		$end = microtime(true);


		$this->line('');
		// Display the memory usage
		$this->info($this->displayMemoryUsage());
		$this->info('Total Records Inserted: ' . number_format($totalRecords, 0, '', ','));
		$this->info('Data seeded successfully!');

		$executionTime = $end - $start;

		$this->info('handle method execution time: ' . round($executionTime, 2) . ' seconds');

		return 0;

	}

	public function processSeed($totalRecords, $batchSize, $connection, $table){

		$start = microtime(true);
		$iterations = intdiv($totalRecords, $batchSize);
		$columnsAndTypes = $this->getColumnsAndTypes($connection, $table);
		$faker = Faker::create();

		for ($i = 0; $i < $iterations; $i++) {
			foreach ($this->generateDataBatch($batchSize, $columnsAndTypes, $faker) as $data) {
				DB::connection($connection)->table($table)->insert($data);
			}
		}

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

	private function generateDataBatch($batchSize, $columns, $faker)
	{
		for ($j = 0; $j < $batchSize; $j++) {
			$data = [];

			foreach ($columns as $column => $type) {
				switch ($type) {
					case 'name':
						$data[$column] = $faker->name;
						break;
					case 'email':
						$data[$column] = $faker->email;
						break;
					case 'address':
						$data[$column] = $faker->address;
						break;
					case 'integer':
					case 'int4':
					case 'float4':
						$data[$column] = $faker->randomNumber();
						break;
					case 'timestamp':
						$data[$column] = $faker->dateTimeThisYear()->format('Y-m-d H:i:s');
						break;
					case 'varchar':
						$data[$column] = $faker->word;
						break;
					case 'text':
						$data[$column] = $faker->text;
						break;
					case 'bool':
						$data[$column] = $faker->boolean;
						break;
					default:
						$data[$column] = $faker->word;
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
			return 'Memory usage: ' . $memoryUsage . ' bytes';
		} elseif ($memoryUsage < 1048576) {
			return 'Memory usage: ' . round($memoryUsage / 1024, 2) . ' KB';
		} else {
			return 'Memory usage: ' . round($memoryUsage / 1048576, 2) . ' MB';
		}
	}
}
