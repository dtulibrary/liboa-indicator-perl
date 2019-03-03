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
    my $rs = $self->{'db'}->select ('id,source', 'records');
    my $rec;
    while ($rec = $self->{'db'}->next ($rs)) {
        $count->{'total'}++;
        $records->{$rec->{'id'}} = {id => $rec->{'id'}, source => $rec->{'source'}};
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
            if ($mxd->pub_status ($id) eq 'p') {
                $records->{$id}{'scoped_published'} = 1;
            } else {
                $records->{$id}{'scoped_published'} = 0;
            }
            if (($records->{$id}{'scoped_type'}) && ($records->{$id}{'scoped_review'}) && ($records->{$id}{'scoped_level'}) && ($records->{$id}{'scoped_published'})) {
                $records->{$id}{'scoped'} = 1;
            } else {
                $records->{$id}{'scoped'} = 0;
            }
            delete ($records->{$id}{'source'});
            $self->{'db'}->update ('records', 'id', $records->{$id});
            $count->{'done'}++;
            if (($count->{'done'} % 5000) == 0) {
                $self->{'oai'}->log ('i', "processed $count->{'done'} records out of $count->{'total'}");
            }
        } else {
            foreach my $fld ('scoped_type', 'scoped_review', 'scoped_level', 'scoped_published', 'scoped') {
                $records->{$id}{$fld} = 0;
            }
            $self->{'oai'}->log ('e', "could not find MXD record for id '$id', $records->{$id}{'source'}");
        }
    }
    $self->{'oai'}->log ('i', "processed $count->{'done'} records out of $count->{'total'}");
    $self->{'oai'}->log ('i', 'done');
    return (1);
}

1;

