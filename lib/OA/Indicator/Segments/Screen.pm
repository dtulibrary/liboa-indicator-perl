package OA::Indicator::Segments::Screen;

use strict;
use warnings;
use OA::Indicator::DB::MXD;

sub new
{
    my ($class, $db, $oai) = @_;
    my $self = {};

    $self->{'db'} = $db;
    $self->{'oai'} = $oai;
    return (bless ($self, $class));
}

sub process
{
    my ($self) = @_;

    my $count = {};
    my $records = {};
    my $rs = $self->{'db'}->select ('id,year,source', 'records');
    my $rec;
    while ($rec = $self->{'db'}->next ($rs)) {
        $count->{'total'}++;
        if (!$rec->{'year'}) {
            $rec->{'year'} = 0;
        }
        $records->{$rec->{'id'}} = {id => $rec->{'id'}, year => $rec->{'year'}, source => $rec->{'source'}};
    }
    $self->{'oai'}->log ('i', "starting screening of $count->{'total'} records");
    my $mxd = new OA::Indicator::DB::MXD ($self->{'db'}, $self->{'oai'});
    foreach my $id (keys (%{$records})) {
        if ($mxd->exists ($id)) {
            $records->{$id}{'screened_issn'} = 0;
            foreach my $issn ($mxd->issn ($id)) {
                if ($issn =~ m/^[0-9]{7}[0-9X]$/) {
                    $records->{$id}{'screened_issn'} = 1;
                    last;
                }
            }
            if ($records->{$id}{'year'} == $self->{'oai'}->arg ('year')) {
                $records->{$id}{'screened_year'} = 1;
            } else {
                $records->{$id}{'screened_year'} = 0;
            }
            if (($records->{$id}{'screened_issn'}) && ($records->{$id}{'screened_year'})) {
                $records->{$id}{'screened'} = 1;
            } else {
                $records->{$id}{'screened'} = 0;
            }
            delete ($records->{$id}{'source'});
            $self->{'db'}->update ('records', 'id', $records->{$id});
            $count->{'done'}++;
            if (($count->{'done'} % 5000) == 0) {
                $self->{'oai'}->log ('i', "processed $count->{'done'} records out of $count->{'total'}");
            }
        } else {
            $self->{'oai'}->log ('e', "could not find MXD record for id '$id', $records->{$id}{'source'}");
        }
    }
    $self->{'oai'}->log ('i', "processed $count->{'done'} records out of $count->{'total'}");
    $self->{'oai'}->log ('i', 'done');
    return (1);
}

1;

