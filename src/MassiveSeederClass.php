<?php

namespace Elminson\MassiveSeeder;

use Faker\Factory as Faker;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

class MassiveSeederCommand extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'db:massive-seed';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Seed a database table with massive data';

    /**
     * Execute the console command.
     *
     * @return int
     */
    public function handle()
    {
        $connection = $this->ask('What connection do you want to use?');
        $table = $this->ask('What table do you want to seed?');

        $columns = DB::connection($connection)->getSchemaBuilder()->getColumnListing($table);

        $faker = Faker::create();

        $data = [];

        foreach ($columns as $column) {
            // You can add more cases for different column types
            switch ($column) {
                case 'name':
                    $data[$column] = $faker->name;
                    break;
                case 'email':
                    $data[$column] = $faker->email;
                    break;
                case 'address':
                    $data[$column] = $faker->address;
                    break;
                default:
                    $data[$column] = $faker->word;
            }
        }

        DB::connection($connection)->table($table)->insert($data);

        $this->info('Data seeded successfully!');

        return 0; // 0 means command was successful
    }
}
