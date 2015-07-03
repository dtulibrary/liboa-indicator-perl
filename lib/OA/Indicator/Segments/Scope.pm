package OA::Indicator::Segments::Scope;

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
    my $rs = $self->{'db'}->select ('id', 'records');
    my $rec;
    while ($rec = $self->{'db'}->next ($rs)) {
        $count->{'total'}++;
        $records->{$rec->{'id'}} = {id => $rec->{'id'}};
    }
    $self->{'oai'}->log ('i', "starting scoping of $count->{'total'} records");
    my $mxd = new OA::Indicator::DB::MXD ($self->{'db'}, $self->{'oai'});
    foreach my $id (keys (%{$records})) {
        if ($mxd->exists ($id)) {
            if ($mxd->type ($id) =~ m/^(dja|djr|dcp)$/) {
                $records->{$id}{'scoped_type'} = 1;
            } else {
                $records->{$id}{'scoped_type'} = 0;
            }
            if ($mxd->review ($id) eq 'pr') {
                $records->{$id}{'scoped_review'} = 1;
            } else {
                $records->{$id}{'scoped_review'} = 0;
            }
            if ($mxd->level ($id) eq 'sci') {
                $records->{$id}{'scoped_level'} = 1;
            } else {
                $records->{$id}{'scoped_level'} = 0;
            }
            if (($records->{$id}{'scoped_type'}) && ($records->{$id}{'scoped_review'}) && ($records->{$id}{'scoped_level'})) {
                $records->{$id}{'scoped'} = 1;
            } else {
                $records->{$id}{'scoped'} = 0;
            }
            $self->{'db'}->update ('records', 'id', $records->{$id});
            $count->{'done'}++;
            if (($count->{'done'} % 5000) == 0) {
                $self->{'oai'}->log ('i', "processed $count->{'done'} records out of $count->{'total'}");
            }
        } else {
#           FIX
            my $rs = $self->{'db'}->select ('source', 'records');
            my $rec = $self->{'db'}->next ($rs);
            $self->{'oai'}->log ('e', "could not find MXD record for id '$id', $rec->{'source'}");
#           $self->{'oai'}->log ('f', 'failed');
#           return (0);
        }
    }
    $self->{'oai'}->log ('i', "processed $count->{'done'} records out of $count->{'total'}");
    $self->{'oai'}->log ('i', 'done');
    return (1);
}

1;

