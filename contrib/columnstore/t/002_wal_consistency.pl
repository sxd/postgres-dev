# Copyright (c) 2026, PostgreSQL Global Development Group

# Every delta-page modification must go through generic WAL: the generic
# resource manager has no masking function, so even an "advisory" unlogged
# write (hint bits, fence flags) makes later WAL records for the page fail
# consistency checking, and a crashed cluster then refuses to recover.
# Run a workload covering inserts, row locks (delta and columnar),
# deletes, and compaction under wal_consistency_checking='Generic', then
# crash and verify recovery replays every record cleanly.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('cs_walcheck');
$node->init;
$node->append_conf('postgresql.conf',
	"wal_consistency_checking = 'Generic'");
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION columnstore');
$node->safe_psql('postgres',
	'CREATE TABLE wl (id int, v text) USING columnstore');
$node->safe_psql('postgres',
	"INSERT INTO wl SELECT g, 'w' || g FROM generate_series(1, 5000) g");

# compaction: fences, consumption, freeze
$node->safe_psql('postgres', 'VACUUM wl');


# crash hard and recover: every Generic record replayed is verified
# against the full-page image wal_consistency_checking attached to it
$node->stop('immediate');
$node->start;

my $count = $node->safe_psql('postgres', 'SELECT count(*) FROM wl');
is($count, 5000, 'data intact after crash recovery');

my $log = slurp_file($node->logfile);
unlike(
	$log,
	qr/inconsistent page found/,
	'no WAL consistency failures during recovery');

$node->stop;
done_testing();
