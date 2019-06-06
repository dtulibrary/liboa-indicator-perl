use 5.020002;
use ExtUtils::MakeMaker;
WriteMakefile(
    NAME              => 'OA::Indicator',
    VERSION_FROM      => 'lib/OA/Indicator.pm',
    PREREQ_PM         => {},
#   ABSTRACT_FROM     => 'lib/OA/Indicator.pm',
    AUTHOR            => 'franck <franck@cvt.dk>',
);

sub MY::postamble
{
    return <<'MAKE_FRAG';
debian: all
	@dpkg-buildpackage -rfakeroot -uc -us

install::
	@mkdir -p $(DESTDIR)/etc/oa-indicator
	@mkdir -p $(DESTDIR)/var/log/oa-indicator
	@mkdir -p $(DESTDIR)/var/lib/oa-indicator/cgi
	@mkdir -p $(DESTDIR)/var/lib/oa-indicator/db
	@mkdir -p $(DESTDIR)/var/lib/oa-indicator/ft
	@mkdir -p $(DESTDIR)/var/lib/oa-indicator/log
	@mkdir -p $(DESTDIR)/var/lib/oa-indicator/nagios
	@mkdir -p $(DESTDIR)/var/lib/oa-indicator/runs
	@mkdir -p $(DESTDIR)/var/lib/oa-indicator/tmp
	@mkdir -p $(DESTDIR)/var/run/oa-indicator
	@install -D -m 775 bin/oa-indicator $(DESTDIR)/usr/bin/oa-indicator
	@for file in sbin/*; \
        do \
            install -D -m 775 $$file $(DESTDIR)/usr/$$file; \
        done
	@for file in cgi/*; \
        do \
            install -D -m 775 $$file $(DESTDIR)/var/lib/oa-indicator/$$file; \
        done
	@install -D -m 664 etc/apache.conf $(DESTDIR)/etc/apache2/conf-enabled/oa-indicator.conf
	@install -D -m 664 etc/whitelist.tab $(DESTDIR)/etc/oa-indicator/whitelist.tab
	@install -D -m 664 etc/labels.tab $(DESTDIR)/etc/oa-indicator/labels.tab
	@install -D -m 664 etc/hosts.tab $(DESTDIR)/etc/oa-indicator/hosts.tab
	@cd etc; \
        for file in html/*; \
        do \
            install -D -m 664 $$file $(DESTDIR)/etc/oa-indicator/$$file; \
        done

distclean::
	rm -rf debian/liboa-indicator-perl
	rm -f debian/files debian/liboa-indicator-perl.substvars \
              build-stamp install-stamp
MAKE_FRAG
}

