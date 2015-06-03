package OA::Indicator;

use strict;
use warnings;
use Fatal qw(open);

our $VERSION = '1.0';

sub new
{
    my ($class, %args) = @_;
    my $self = \%args;

    $self->{'year-from'} = 2013;
    $self->{'year-to'} = 2019;

    return (bless ($self, $class));
}

sub valid_year
{
    my ($self, $year) = @_;

    if (($year < $self->{'year-from'}) || ($year > $self->{'year-to'})) {
        return (0);
    } else {
        return (1);
    }
}

sub valid_year_range
{
    my ($self) = @_;

    return ($self->{'year-from'} . '-' . $self->{'year-to'});
}

sub elapse
{
    my ($self, $sec) = @_;

    my $day = int ($sec / (3600 * 24));
    $sec = ($sec % (3600 * 24));
    my $hour = int ($sec / 3600);
    $sec = ($sec % 3600);
    my $min = int ($sec / 60);
    $sec = ($sec % 60);
    if ($day) {
        if ($day == 1) {
            return (sprintf ('1 day, %2d:%02d:%02d', $hour, $min, $sec));
        } else {
            return (sprintf ('%d days, %2d:%02d:%02d', $day, $hour, $min, $sec));
        }
    } else {
        return (sprintf ('%2d:%02d:%02d', $hour, $min, $sec));
    }
}

sub verbose
{
    my ($self, $verbose) = @_;

    if (defined ($verbose)) {
        $self->{'verbose'} = $verbose;
    }
    return ($self->{'verbose'});
}

sub log_file
{
    my ($self, $file) = @_;

    if (defined ($file)) {
        $self->{'log_file'} = $file;
    }
    return ($self->{'log_file'});
}

sub log_main
{
    my ($self, $level, $msg, @param) = @_;
    
    $self->_log ('log_main', '/var/log/oa-indicator.log', $level, $msg, @param);
}

sub log
{
    my ($self, $level, $msg, @param) = @_;
    
    if (!exists ($self->{'log_file'})) {
        die ('log_file undefined');
    }
    $self->_log ('log', $self->{'log_file'}, $level, $msg, @param);
}

sub _log
{
    my ($self, $type, $file, $level, $msg, @param) = @_;
    my ($sec, $min, $hour, $day, $mon, $year) = localtime (time);
    
    my $fou;
    if (exists ($self->{$type})) {
        $fou = $self->{$type};
    } else {
        open ($fou, ">> $file");
        $self->{$type} = $fou;
    }
    printf ($fou "%04d-%02d-%02d %02d:%02d:%02d %s " . $msg . "\n", 1900 + $year, $mon + 1, $day, $hour, $min, $sec, $level, @param);
    if ($self->{'verbose'}) {
        printf (STDERR "%04d-%02d-%02d %02d:%02d:%02d %s " . $msg . "\n", 1900 + $year, $mon + 1, $day, $hour, $min, $sec, $level, @param);
    }
}

1;

