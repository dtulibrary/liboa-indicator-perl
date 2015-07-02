package OA::Indicator::DB;

use strict;
use warnings;
use DB::SQLite;

our $VERSION = '1.0';

sub new
{
    my ($class, $year, $type) = @_;
    my $self = {};

    if ($year !~ m/^[0-9]{4}$/) {
        die ("fatal: argument should be 4 digits");
    }
    $self->{'year'} = $year;
    $self->{'year'} =~ s/[^0-9]//g;
    if (($self->{'year'} < 2000) || ($self->{'year'} > 2030)) {
        die ("fatal: invalid year: $year, valid years are 2000-2030");
    }
    $self->{'type'} = lc ($type);
    $self->{'type'} =~ s/[^a-z]//g;
    if ($self->{'type'} !~ m/^(devel|test|prod)$/) {
        die ("fatal: invalid run type: $type, valid types are: devel | test | prod");
    }
    $self->{'root'} = '/var/lib/oa-indicator/db';
    $self->{'segments'} = [qw(load scope screen fetch verify classify report preserve)];
    foreach my $seg (@{$self->{'segments'}}) {
        $self->{'valid_segment'}{$seg} = 1;
    }
    return (bless ($self, $class));
}

sub create
{   
    my ($self, $desc) = @_;
    my ($db);

    $self->{'rundb'} = $db = new DB::SQLite ("$self->{'root'}/runs.sqlite3", DieError => 1);
    $db->sql ('create table if not exists runs (
                   id              integer primary key,
                   year            integer,
                   type            text,
                   description     text,
                   start           integer,
                   end             integer,
                   success         integer,
                   errors          text
               )');
    $db->sql ('create index if not exists runs_year on runs (year)');
    $db->sql ('create index if not exists runs_type on runs (type)');
    $db->sql ('create table if not exists segments (
                   id              integer primary key,
                   run             integer,
                   name            text,
                   start           integer,
                   end             integer,
                   success         integer,
                   errors          text
               )');
    $db->sql ('create index if not exists segments_run on segments (run)');
    $db->insert ('runs', {year => $self->{'year'}, type => $self->{'type'}, description => $desc, start => time, end => 0});
    $self->{'run'} = $db->lastid;
    my $run = sprintf ('%03d', $self->{'run'});
    $self->{'db'} = $db = new DB::SQLite ("$self->{'root'}/run_$run.$self->{'year'}.$self->{'type'}.sqlite3", DieError => 1, cacheUpdates => 1000);
    return ($self->{'db'});
}

sub open
{
    my ($self) = @_;
    my ($db);

    $self->{'rundb'} = $db = new DB::SQLite ("$self->{'root'}/runs.sqlite3", DieError => 1);
    my $rs = $db->select ('id', 'runs', "year=$self->{'year'} and type='$self->{'type'}' and end=0", 'order by id desc');
    my $rec;
    while ($rec = $db->next ($rs)) {
        if ($rec->{'id'}) {
            $self->{'run'} = $rec->{'id'};
            last;
        }
    }
    if (!$self->{'run'}) {
        die ("fatal: could not find an open run for year=$self->{'year'} and type=$self->{'type'}");
    }
    my $run = sprintf ('%03d', $self->{'run'});
    $self->{'db'} = $db = new DB::SQLite ("$self->{'root'}/run_$run.$self->{'year'}.$self->{'type'}.sqlite3", DieError => 1, cacheUpdates => 1000);
    return ($self->{'db'});
}

sub db
{
    my ($self) = @_;

    return ($self->{'db'});
}

sub run
{
    my ($self) = @_;

    return ($self->{'run'});
}

sub segment_start
{
    my ($self, $segment) = @_;

    if (!$self->segment_validate ($segment)) {
        die ("fatal: invalid segment: $segment, valid segments are: " . join (' | ', $self->segments));
    }
    $self->{'rundb'}->insert ('segments', {run => $self->{'run'}, name => $segment, start => time});
}

sub segment_end
{
    my ($self, $segment, $success, @errors) = @_;

    if (!$self->segment_validate ($segment)) {
        die ("fatal: invalid segment: $segment, valid segments are: " . join (' | ', $self->segments));
    }
    my $rs = $self->{'rundb'}->select ('id', 'segments', "run=$self->{'run'} and name='$segment'");
    my $rc;
    my $rec;
    my $count = 0;
    while ($rc = $self->{'rundb'}->next ($rs)) {
        $rec = $rc;
        $count++;
    }
    if ($count != 1) {
        die ("fatal: error while ending segment, $count match for segment '$segment' in run $self->{'run'}");
    }
    $rec->{'end'} = time;
    if ($success) {
        $rec->{'success'} = 1;
        $rec->{'errors'} = '';
    } else {
        $rec->{'success'} = 0;
        foreach my $error (@errors) {
            $error =~ s/[\t\r\n\s]+/ /g;
        }
        $rec->{'errors'} = join ("\t", @errors);
    }
    $self->{'rundb'}->update ('segments', 'id', $rec);
}

sub segment_validate
{
    my ($self, $segment) = @_;

    if (exists ($self->{'valid_segment'}{$segment})) {
        return (1);
    } else {
        return (0);
    }
}

sub segments
{
    my ($self) = @_;

    return (@{$self->{'segments'}});
}

sub close
{
    my ($self, $success, @errors) = @_;

    my $rec = {id => $self->{'run'}, end => time};
    if ($success) {
#       FIX : we should add a check that all segments were successful before declaring a success.
        $rec->{'success'} = 1;
        $rec->{'errors'} = '';
    } else {
        $rec->{'success'} = 0;
        foreach my $error (@errors) {
            $error =~ s/[\t\r\n\s]+/ /g;
        }
        $rec->{'errors'} = join ("\t", @errors);
    }
    $self->{'rundb'}->update ('runs', 'id', $rec);
}

sub reuse
{
    my ($self) = @_;
    my ($db);

    $self->{'rundb'} = $db = new DB::SQLite ("$self->{'root'}/runs.sqlite3", DieError => 1);
    my $rs = $db->select ('id', 'runs', "year=$self->{'year'} and type='$self->{'type'}' and success=1", 'order by id desc');
    my $rec;
    while ($rec = $db->next ($rs)) {
        if ($rec->{'id'}) {
            $self->{'run'} = $rec->{'id'};
            last;
        }
    }
    if (!$self->{'run'}) {
        die ("fatal: could not find a completed run for year=$self->{'year'} and type=$self->{'type'}");
    }
    my $run = sprintf ('%03d', $self->{'run'});
    $self->{'db'} = $db = new DB::SQLite ("$self->{'root'}/run_$run.$self->{'year'}.$self->{'type'}.sqlite3", DieError => 1);
    return ($self->{'db'});
}

1;

