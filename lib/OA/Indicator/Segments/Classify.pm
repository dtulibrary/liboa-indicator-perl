package OA::Indicator::Segments::Classify;

use strict;
use warnings;
use DB::SQLite;
use OA::Indicator::DB::DOAR;

sub new
{
    my ($class, $db, $oai) = @_;
    my $self = {};

    $self->{'db'} = $db;
    $self->{'oai'} = $oai;
    $self->{'ft'} = new DB::SQLite ("/var/lib/oa-indicator/db/fulltext.sqlite3", DieError => 1);
    $self->{'wl'} = new OA::Indicator::DB::DOAR ($db, $oai);
    return (bless ($self, $class));
}

sub process
{
    my ($self) = @_;

    my $count = {};
    my $records = {};
    my $publications = {};
    my $rec;
#   Getting records to classify
    my $rs = $self->{'db'}->select ('id,doaj_issn,bfi_level,romeo_color,romeo_issn,fulltext_link,fulltext_link_oa,fulltext_downloaded,fulltext_verified,dedupkey,' .
                                    'research_area,bfi_research_area',
                                    'records',
                                    'scoped = 1 and screened = 1');
    while ($rec = $self->{'db'}->next ($rs)) {
        $count->{'total'}++;
        $records->{$rec->{'id'}} = $rec;
        $publications->{$rec->{dedupkey}}{$rec->{'id'}} = $rec;
    }
#   Setting publication research area
    foreach my $dkey (keys (%{$publications})) {
        my $bfi_research_area = '';
        my $bfi_research_area_error = 0;
        my $mra = {hum => 0, med => 0, sci => 0, soc => 0};
        foreach my $id (keys (%{$publications->{$dkey}})) {
            if ($publications->{$dkey}{$id}{'bfi_research_area'}) {
                if (($bfi_research_area) && ($bfi_research_area ne $publications->{$dkey}{$id}{'bfi_research_area'})) {
                    $bfi_research_area_error = 1;
                } else {
                    $bfi_research_area = $publications->{$dkey}{$id}{'bfi_research_area'};
                }
            }
            $mra->{$publications->{$dkey}{$id}{'research_area'}}++;
        }
        my $pub_research_area;
        if (($bfi_research_area) && (!$bfi_research_area_error)) {
            $pub_research_area = $bfi_research_area;
        } else {
            my @mra = sort {$mra->{$a} <=> $mra->{$b}} keys (%{$mra});
            $pub_research_area = pop (@mra);
        }
        foreach my $id (keys (%{$publications->{$dkey}})) {
            $records->{$id}{'pub_research_area'} = $pub_research_area;
        }
    }
    $self->{'oai'}->log ('i', "starting classifying of $count->{'total'} records");
    foreach my $id (keys (%{$records})) {
        my $rec = $records->{$id};
        if ($rec->{'fulltext_link_oa'}) {
            $count->{'oa-link'}++;
            my $rc;
            my @url = ();
            my $rs = $self->{'ft'}->select ('type,url', 'fulltext_requests', "dsid='$id'");
            while ($rc = $self->{'ft'}->next ($rs)) {
                if ((defined ($rc->{'url'})) && ($rc->{'url'} !~ m/^[\s\n\r]*$/)) {
                    if ($rc->{'type'} eq 'loc') {
                        push (@url, $rc->{'url'});
                    } elsif ($rc->{'type'} eq 'rem') {
                        my $uri;
                        if ($uri = $self->{'wl'}->valid ($rc->{'url'})) {
                            push (@url, $rc->{'url'});
                            $self->{'oai'}->log ('i', "valid URL based on '%s': %s", $uri, $rc->{'url'});
                        } else {
                            $self->{'oai'}->log ('i', "invalid URL: %s", $rc->{'url'});
                        }
                    } else {
                        $self->{'oai'}->log ('e', "unknown type ($rc->{'type'}) for URL: $rc->{'url'}");
                    }
                }
            }
            foreach my $u (@url) {
                $count->{'oa-url'}++;
                $rs = $self->{'ft'}->select ('http_code,success,size,error_message,pdf_pages', 'fulltext', "url='$u'");
                while ($rc = $self->{'ft'}->next ($rs)) {
                    $count->{'ft-rec'}++;
                    if ($rc->{'success'}) {
                        $count->{'ft-success'}++;
                        $rec->{'fulltext_downloaded'} = 1;
                        $rec->{'fulltext_verified'} = 1;
                        if (($rc->{'pdf_pages'}) && ($rc->{'pdf_pages'} > 0)) {
                            $rec->{'fulltext_pdf'} = 1;
                        }
                    } else {
#                       Special check because we want a very wide understanding of valid fulltext this year
                        if (($rc->{'http_code'} eq '200') && ($rc->{'size'} > 0) && ($rc->{'error_message'} eq 'PDF error')) {
                            $count->{'ft-success-2015'}++;
                            $rec->{'fulltext_downloaded'} = 1;
                            $rec->{'fulltext_verified'} = 1;
                        }
                    }
                }
            }
        }
        if (($rec->{'doaj_issn'}) && ($rec->{'bfi_level'})) {
            $rec->{'class'} = 'realized';
            $rec->{'class_reasons'} = ['golden'];
        } else {
            $rec->{'class_reasons'} = [];
            if (!$rec->{'doaj_issn'}) {
                push (@{$rec->{'class_reasons'}}, 'not-doaj');
            }
            if (!$rec->{'bfi_level'}) {
                push (@{$rec->{'class_reasons'}}, 'not-bfi');
            }
            if ($rec->{'romeo_color'} =~ m/(green|blue|yellow)/) {
                push (@{$rec->{'class_reasons'}}, 'romeo');
                if ($rec->{'fulltext_verified'}) {
                   $rec->{'class'} = 'realized';
                   push (@{$rec->{'class_reasons'}}, 'fulltext');
                } else {
                   $rec->{'class'} = 'unused';
                   if ($rec->{'fulltext_link_oa'}) {
                       push (@{$rec->{'class_reasons'}}, 'fulltext-error');
                   } else {
                       if ($rec->{'fulltext_link'}) {
                           push (@{$rec->{'class_reasons'}}, 'no-oa-fulltext');
                       } else {
                           push (@{$rec->{'class_reasons'}}, 'no-fulltext');
                       }
                   }
                }
            } else {
                push (@{$rec->{'class_reasons'}}, 'not-romeo');
                if ($rec->{'fulltext_verified'}) {
                   $rec->{'class'} = 'realized';
                   push (@{$rec->{'class_reasons'}}, 'fulltext');
                } else {
                   $rec->{'class'} = 'unclear';
                   if ($rec->{'fulltext_link_oa'}) {
                       push (@{$rec->{'class_reasons'}}, 'fulltext-error');
                   } else {
                       if ($rec->{'fulltext_link'}) {
                           push (@{$rec->{'class_reasons'}}, 'no-oa-fulltext');
                       } else {
                           push (@{$rec->{'class_reasons'}}, 'no-fulltext');
                       }
                   }
                }
            }
        }
        $rec->{'class_reasons'} = join (',', @{$rec->{'class_reasons'}});
        $rec->{'pub_class'} = $rec->{'class'};
        $rec->{'pub_class_reasons'} = $rec->{'class_reasons'};
        $self->{'db'}->update ('records', 'id', $rec);
        $count->{'done'}++;
        if (($count->{'done'} % 5000) == 0) {
            $self->{'oai'}->log ('i', "processed $count->{'done'} records out of $count->{'total'}");
        }
    }
    $self->{'oai'}->log ('i', "processed $count->{'done'} records out of $count->{'total'}");
    foreach my $f (sort (keys (%{$count}))) {
        $self->{'oai'}->log ('d', "count: %-20s %d", $f, $count->{$f});
    }
    $self->{'oai'}->log ('i', "updating publication classes");
    foreach my $dkey (keys (%{$publications})) {
        my $class = '';
        my $class_reasons = '';
        foreach my $id (keys (%{$publications->{$dkey}})) {
            if ($publications->{$dkey}{$id}{'class'} eq 'realized') {
                $class = 'realized';
                $class_reasons = $publications->{$dkey}{$id}{'class_reasons'};
            } else {
                if ($publications->{$dkey}{$id}{'class'} eq 'unused') {
                    if ($class ne 'realized') {
                        $class = 'unused';
                        $class_reasons = $publications->{$dkey}{$id}{'class_reasons'};
                    }
                } else {
                    if (!$class) {
                        $class = 'unclear';
                        $class_reasons = $publications->{$dkey}{$id}{'class_reasons'};
                    }
                }
            }
        }
        foreach my $id (keys (%{$publications->{$dkey}})) {
            if ($publications->{$dkey}{$id}{'pub_class'} ne $class) {
                $publications->{$dkey}{$id}{'pub_class'} = $class;
                $publications->{$dkey}{$id}{'pub_class_reasons'} = $class_reasons;
            }
            $self->{'db'}->update ('records', 'id', $publications->{$dkey}{$id});
        }
    }
    $self->{'oai'}->log ('i', 'done');
    return (1);
}

1;

