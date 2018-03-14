package OA::Indicator::DB;

use strict;
use warnings;
use DB::SQLite;

our $VERSION = '1.0';

sub new
{
    my ($class) = @_;
    my $self = {};

    $self->{'root'} = '/var/lib/oa-indicator/db';
    $self->{'rundb'} = new DB::SQLite ("$self->{'root'}/runs.sqlite3", DieError => 1);
    $self->{'segments'} = [qw(load scope screen fetch verify classify report preserve)];
    foreach my $seg (@{$self->{'segments'}}) {
        $self->{'valid_segment'}{$seg} = 1;
    }
    return (bless ($self, $class));
}

sub validate_year
{
    my ($self, $year) = @_;

    $year =~ s/[^0-9]//g;
    if ($year !~ m/^[0-9]{4}$/) {
        die ("fatal: year should be 4 digits");
    }
    if (($year < 2000) || ($year > 2030)) {
        die ("fatal: invalid year: $year, valid years are 2000-2030");
    }
    $self->{'year'} = $year;
}

sub validate_type
{
    my ($self, $type) = @_;

    $self->{'type'} = lc ($type);
    $self->{'type'} =~ s/[^a-z]//g;
    if ($self->{'type'} !~ m/^(devel|test|prod)$/) {
        die ("fatal: invalid run type: $type, valid types are: devel | test | prod");
    }
}

sub create
{   
    my ($self, $repo, $year, $type, $desc) = @_;
    my $db = $self->{'rundb'};

    $self->validate_year ($year);
    $self->validate_type ($type);
    $db->sql ('create table if not exists runs (
                   id              text primary key,
                   year            integer,
                   type            text,
                   run             integer,
                   description     text,
                   start           integer,
                   end             integer,
                   success         integer,
                   released        integer,
                   errors          text,
                   repository      text
               )');
    $db->sql ('create index if not exists runs_year on runs (year)');
    $db->sql ('create index if not exists runs_type on runs (type)');
    $db->sql ('create index if not exists runs_run on runs (run)');
    $db->sql ('create table if not exists segments (
                   id              integer primary key,
                   run             text,
                   name            text,
                   start           integer,
                   end             integer,
                   success         integer,
                   errors          text
               )');
    $db->sql ('create index if not exists segments_run on segments (run)');
    $self->{'run'} = $self->new_run ($self->{'year'}, $self->{'type'});
    $self->{'id'} = $self->{'year'} . '.' . $self->{'type'} . '.' . sprintf ('%03d', $self->{'run'});
    $db->insert ('runs', {id => $self->{'id'}, year => $self->{'year'}, type => $self->{'type'}, run => $self->{'run'}, description => $desc, start => time,
                          end => 0, success => 0, released => 0, repository => $repo});
    $self->{'db'} = new DB::SQLite ("$self->{'root'}/run_$self->{'id'}.sqlite3", DieError => 1, cacheUpdates => 1000);
    return ($self->{'db'});
}

sub open
{
    my ($self, $year, $type) = @_;
    my $db = $self->{'rundb'};

    $self->validate_year ($year);
    $self->validate_type ($type);
    my $rs = $db->select ('id,run', 'runs', "year=$self->{'year'} and type='$self->{'type'}' and end=0", 'order by run desc');
    my $rec;
    while ($rec = $db->next ($rs)) {
        if ($rec->{'run'}) {
            $self->{'id'} = $rec->{'id'};
            $self->{'run'} = $rec->{'run'};
            last;
        }
    }
    if (!$self->{'run'}) {
        die ("fatal: could not find an open run for year=$self->{'year'} and type=$self->{'type'}");
    }
    $self->{'db'} = new DB::SQLite ("$self->{'root'}/run_$self->{'id'}.sqlite3", DieError => 1, cacheUpdates => 1000);
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

sub id
{
    my ($self) = @_;

    return ($self->{'id'});
}

sub year
{
    my ($self) = @_;

    return ($self->{'year'});
}

sub type
{
    my ($self) = @_;

    return ($self->{'type'});
}

sub segment_start
{
    my ($self, $segment) = @_;
    my $db = $self->{'rundb'};

    if (!$self->segment_validate ($segment)) {
        die ("fatal: invalid segment: $segment, valid segments are: " . join (' | ', $self->segments));
    }
    $db->insert ('segments', {run => $self->{'id'}, name => $segment, start => time, end => 0, success => 0});
}

sub segment_end
{
    my ($self, $segment, $success, @errors) = @_;
    my $db = $self->{'rundb'};

    if (!$self->segment_validate ($segment)) {
        die ("fatal: invalid segment: $segment, valid segments are: " . join (' | ', $self->segments));
    }
    my $rs = $db->select ('id', 'segments', "run='$self->{'id'}' and name='$segment' and end=0");
    my $rc;
    my $rec;
    my $count = 0;
    while ($rc = $db->next ($rs)) {
        $rec = $rc;
        $count++;
    }
    if ($count != 1) {
        die ("fatal: error while ending segment, $count match for segment '$segment' in run $self->{'id'}");
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
    $db->update ('segments', 'id', $rec);
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
    my $db = $self->{'rundb'};

    if (!exists ($self->{'id'})) {
        die ("fatal: cannot call close without calling create or open first");
    }
    my $rec = {id => $self->{'id'}, end => time};
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
    if (($rec->{'success'}) && ($self->{'type'} ne 'prod')) {
        $rec->{'released'} = 1;
    }
    $db->update ('runs', 'id', $rec);
}

sub reuse
{
    my ($self, $year, $type, $run) = @_;
    my $db = $self->{'rundb'};

    $self->validate_year ($year);
    $self->validate_type ($type);
    my $rs = $db->select ('id,run', 'runs', "year=$self->{'year'} and type='$self->{'type'}' and success=1", 'order by run desc');
    my $rec;
    while ($rec = $db->next ($rs)) {
        if ($rec->{'id'}) {
            if ((!$run) || ($run == $rec->{'run'})) {
                $self->{'id'} = $rec->{'id'};
                $self->{'run'} = $rec->{'run'};
                last;
            }
        }
    }
    if (!$self->{'run'}) {
        die ("fatal: could not find a completed run for year=$self->{'year'} and type=$self->{'type'}");
    }
    $self->{'db'} = new DB::SQLite ("$self->{'root'}/run_$self->{'id'}.sqlite3", DieError => 1);
    return ($self->{'db'});
}

sub release
{
    my ($self, $release) = @_;
    my $db = $self->{'rundb'};

    if (!exists ($self->{'id'})) {
        die ("fatal: cannot call release without calling reuse first");
    }
    my $rec = {id => $self->{'id'}};
    $rec->{'released'} = $release;
    $db->update ('runs', 'id', $rec);
}

sub new_run
{
    my ($self, $year, $type) = @_;
    my $db = $self->{'rundb'};

    my $rs = $db->select ('run', 'runs', "year=$year and type='$type'", 'order by run desc');
    my $rec;
    if ($rec = $db->next ($rs)) {
        return ($rec->{'run'} + 1);
    } else {
        return (1);
    }
}

sub run_years
{
    my ($self, $types, $success) = @_;
    my $db = $self->{'rundb'};
    my $years = {};

    my $w = $self->run_where ($types, $success);
    my $rs = $db->select ('year', 'runs', $w);
    my $rec;
    while ($rec = $db->next ($rs)) {
        $years->{$rec->{'year'}} = 1;
    }
    return (sort (keys (%{$years})));
}

sub run_types
{
    my ($self, $year, $types, $success) = @_;
    my $db = $self->{'rundb'};
    my $ret = {};

    my $w = $self->run_where ($types, $success);
    if ($w) {
        $w .= ' and year=' . $year;
    } else {
        $w = 'year=' . $year;
    }
    my $rs = $db->select ('type', 'runs', $w);
    my $rec;
    while ($rec = $db->next ($rs)) {
        $ret->{$rec->{'type'}} = 1;
    }
    return (sort (keys (%{$ret})));
}

sub run_runs
{
    my ($self, $year, $type, $success) = @_;
    my $db = $self->{'rundb'};
    my @runs = ();

    my $w = $self->run_where ('', $success);
    if ($w) {
        $w .= " and year=$year and type='$type'";
    } else {
        $w = "year=$year and type='$type'";
    }
    my $rs = $db->select ('run', 'runs', $w, 'order by run');
    my $rec;
    while ($rec = $db->next ($rs)) {
        push (@runs, $rec->{'run'});
    }
    return (@runs);
}

sub run_where
{
    my ($self, $types, $success) = @_;

    my $w = '';
    if ($types) {
        if ($types ne 'all') {
            foreach my $type (split (',', $types)) {
                if ($w) {
                    $w .= ' or ';
                }
                $w .= "type='$type'"
            }
            if ($w) {
                $w = '(' . $w . ')';
            }
        }
    }
    if ($success) {
        if ($w) {
            $w .= ' and success=1';
        } else {
            $w = 'success=1';
        }
    }
    return ($w);
}

sub run_desc
{
    my ($self, $year, $type, $run) = @_;
    my $db = $self->{'rundb'};

    my $id = $year . '.' . $type . '.' . sprintf ('%03d', $run);
    my $rs = $db->select ('description', 'runs', "id='$id'");
    my $rec;
    if ($rec = $db->next ($rs)) {
        return ($rec->{'description'});
    } else {
        return ('');
    }
}

sub run_info
{
    my ($self, $year, $type, $run) = @_;
    my $db = $self->{'rundb'};

    my $id = $year . '.' . $type . '.' . sprintf ('%03d', $run);
    my $rs = $db->select ('description,repository,start,end,success,released,errors', 'runs', "id='$id'");
    my $rec;
    if ($rec = $db->next ($rs)) {
        return ($rec);
    } else {
        return ({});
    }
}

sub run_db
{
    my ($self, $year, $type, $run) = @_;
    my $db = $self->{'rundb'};

    my $rec;
    if ($year) {
        if ($type) {
            if ($run) {
                my $rs = $db->select ('year,type,run', 'runs', "year=$year and type='$type' and run=$run", 'order by start desc');
                $rec = $db->next ($rs);
            } else {
                my $rs = $db->select ('year,type,run', 'runs', "year=$year and type='$type'", 'order by start desc');
                $rec = $db->next ($rs);
            }
        } else {
            my $rs = $db->select ('year,type,run', 'runs', "year=$year", 'order by start desc');
            $rec = $db->next ($rs);
        }
    } else {
        my $rs = $db->select ('year,type,run', 'runs', '', 'order by start desc');
        $rec = $db->next ($rs);
    }
    if ($rec) {
        my $file = sprintf ('/var/lib/oa-indicator/db/run_%04d.%s.%03d.sqlite3', $rec->{'year'}, $rec->{'type'}, $rec->{'run'});
        if (-e $file) {
            return ($file);
        } else {
            die ("fatal: could not find db with year: $year, type: $type, run: $run, file: $file");
        }
    } else {
        return (undef);
    }
}

1;

