package OA::Indicator::Segments::Classify;

use strict;
use warnings;
use DB::SQLite;
use OA::Indicator::DB::MXD;

sub new
{
    my ($class, $db, $oai) = @_;
    my $self = {};

    $self->{'db'} = $db;
    $self->{'oai'} = $oai;
    $self->{'ft'} = new DB::SQLite ("/var/lib/oa-indicator/db/fulltext.sqlite3", DieError => 1);
    return (bless ($self, $class));
}

sub process
{
    my ($self) = @_;

    my $count = {};
    my $records = {};
    my $rec;
    my $rs = $self->{'db'}->select ('id,doaj_issn,bfi_level,romeo_color,fulltext_link,fulltext_link_oa,fulltext_downloaded,fulltext_verified',
                                    'records',
                                    'scoped = 1 and screened = 1');
    while ($rec = $self->{'db'}->next ($rs)) {
        $count->{'total'}++;
        $records->{$rec->{'id'}} = $rec;
    }
    $self->{'oai'}->log ('i', "starting classifying of $count->{'total'} records");
    foreach my $id (keys (%{$records})) {
        my $rec = $records->{$id};
        if ($rec->{'fulltext_link_oa'}) {
            $count->{'oa-link'}++;
            my $rc;
            my @url = ();
            my $rs = $self->{'ft'}->select ('url', 'fulltext_requests', "dsid='$id'");
            while ($rc = $self->{'ft'}->next ($rs)) {
                if ((defined ($rc->{'url'})) && ($rc->{'url'} !~ m/^[\s\n\r]*$/)) {
                    push (@url, $rc->{'url'});
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
                        if ($rc->{'pdf_pages'} > 0) {
                            $rec->{'fulltext_pdf'} = 1;
                        }
                    } else {
#                       Special check because we want a very wide understanding of valid fulltext this year
                        if (($rc->{'http_code'} eq '200') && ($rc->{'size'} > 0) && ($rc->{'error_message'} eq 'PDF error')) {
                            $count->{'ft-success-2014'}++;
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
    $self->{'oai'}->log ('i', 'done');
    return (1);
}

1;

