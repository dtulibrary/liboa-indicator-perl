package OA::Indicator::DB;

use strict;
use warnings;

our $VERSION = '1.0';

sub new
{
    my ($class, $year) = @_;
    my $self = {};

    if ($year !~ m/^[0-9]{4}$/) {
        die ("fatal: argument should be 4 digits");
    }
    $self->{'year'} = $year;
    $self->{'root'} = '/var/lib/oa-indicator/db/';
    return (bless ($self, $class));
}

sub create
{   
    my ($self) = @_;

    my $db = new DB::SQLite ($self->{'root'} . 'OAindicator.db', DieError => 1);
    $db->sql ('create table if not exists runs (
                   id       integer primary key,
                   year     integer,
                   start    integer,
                   end      integer
               )');
    $db->sql ('create table if not exists years (
                   year    integer primary key,
                   updated integer,
                   run     integer
               )');
    $db->insert ('runs', {year => $self->{'year'}, start => time});
    my $run = $db->lastid;
    $self->{'run'} = $run;
    $run = sprintf ('%03d', $run);
    $db = new DB::SQLite ($self->{'root'} . "OAindicator_$run.db", DieError => 1, cacheUpdates => 1000);
    $db->sql ('create table if not exists records (
                   id        text primary key,
                   source_id text,                                                                                                                
                   stamp     integer,
                   date      text,
                   status    text,
                   year      integer,
                   type      text,
                   level     text,
                   review    text,
                   mods      text,
                   mxd       text
               )');
    return ($self->{'db'} = $db);
}

sub close
{
    my ($self) = @_;

    my $db = new DB::SQLite ($self->{'root'} . 'OAindicator.db', DieError => 1);
    $db->update ('runs', 'id', {id => $self->{'run'}, end => time});
    my $res = $db->select ('*', 'years', "year=$self->{'year'}");
    my $rec;
    if ($rec = $db->next ($res)) {
        $db->update ('years', 'year', {year => $self->{'year'}, updated => time, run => $self->{'run'}});
    } else {
        $db->insert ('years', {year => $self->{'year'}, updated => time, run => $self->{'run'}});
    }
}

sub reuse
{
    my ($self) = @_;

    my $db = new DB::SQLite ($self->{'root'} . 'OAindicator.db', DieError => 1);
    my $res = $db->select ('run', 'years', "year=$year");
    my $rec;
    if (!($rec = $db->next ($res))) {
        die ("fatal: could not find run for year $year");
    }
    my $run = sprintf ('%03d', $rec->{'run'});
    $db = new DB::SQLite ($self->{'root'} . "OAindicator_$run.db", DieError => 1);
    return ($self->{'db'} = $db);
}

1;

