package OA::Indicator::Segments::Unpaywall;

use strict;
use warnings;
use OA::Indicator::DB::Unpaywall;

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
    my ($self, $type) = @_;

    my $upw = new OA::Indicator::DB::Unpaywall ($self->{'db'}, $self->{'oai'});
    $upw->create ();
    my $rs = $self->{'db'}->select ('id,doi', 'records', 'scoped=1 and screened=1 and doi!="" and doi is not null');
    my $rec;
    my $recs = {};
    while ($rec = $self->{'db'}->next ($rs)) {
        $upw->request ($rec->{'doi'});
        if (exists ($recs->{$rec->{'doi'}})) {
            push (@{$recs->{$rec->{'doi'}}}, $rec);
        } else {
            $recs->{$rec->{'doi'}} = [$rec];
        }
    }
    if (!$upw->harvest ($type)) {
        return (0);
    }
    $self->{'oai'}->log ('i', 'updating records...');
    my @DOI = $upw->doi_list ();
    my $total = $#DOI + 1;
    my $done = 0;
    my $records = 0;
    foreach my $doi (@DOI) {
        if (!exists ($recs->{$doi})) {
            next;
        }
        my $rec = $upw->doi_list ($doi);
        foreach my $rc (@{$recs->{$doi}}) {
            if ($rec->{'pub'}) {
                $rc->{'upw_pub'} = $rec->{'pub'};
            } else {
                $rc->{'upw_pub'} = '';
            }
            if ($rec->{'rep'}) {
                $rc->{'upw_rep'} = $rec->{'rep'};
            } else {
                $rc->{'upw_rep'} = '';
            }
            if ($rec->{'msg'}) {
                $rc->{'upw_reasons'} = $rec->{'msg'};
            } else {
                $rc->{'upw_reasons'} = '';
            }
            $self->{'db'}->update ('records', 'id', $rc);
            $records++;
        }
        $done++;
        if (($done % 100) == 0) {
            $self->{'oai'}->log ('i', 'processed %d DOI of %d, %d records updated', $done, $total, $records);
        }
    }
    $self->{'oai'}->log ('i', 'done');
}

1;

