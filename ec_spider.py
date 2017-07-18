from HTMLParser import HTMLParser
from operator import itemgetter
from profiler import profile
from scrapy import log
from scrapy import Spider, FormRequest, Request
from scrapy.contrib.loader import ItemLoader
from scrapy.contrib.loader.processor import MapCompose, Join
from scrapy.exceptions import CloseSpider
import settings
from items import VoterItem, VoterItem_Fields
from models import Voter, DBSession, Sarawak2014q1, Voter2, Dm, Locality, Sarawak2014q1Resolved
from sqlalchemy import func

RESULT_ROWS = 3
MAX_RESTARTS = 30
field_prefix = 'dfnokp'
working_dmids = []


class EC_Spider(Spider):
    name = settings.EC_BOT_NAME
    #allowed_domains = ["spr.gov.my"]
    start_urls = ('http://103.8.163.104/semakdm/semakandm.asp',)
    postdata_prefix = 'dfnokp'
    result_table_row = "//th[text()='BIL']/../../tr[position()>1]"

    def __init__(self, dm=None, statename=None, par=None, limit=100, id_locale=False, year=None):
        self.par = par
        self.limit = limit
        self.statename = statename
        self.restarts = 1
        self.fields_counter = 3
        self.dm = dm
        self.identify_locality = id_locale
        self.year = year

        assert self.year is not None

        Spider.__init__(self)

    def parse(self, response):
        if self.fields_counter < RESULT_ROWS:
            self.fields_counter += 1

            return FormRequest.from_response(
                response,
                clickdata={"name": "Tambah"},
                callback=self.parse
            )

        captcha = response.selector.re("document.write\('<span class.+>(.+)</span>")
        parser = HTMLParser()
        captcha = map(lambda x: parser.unescape(x), captcha)
        captcha_string = str.join('', captcha)

        if len(captcha_string) != 3:
            raise CloseSpider("unable to decode captcha")

        if self.year == '2015':
            nrics = self.get_nrics_2015()
        else:
            nrics = self.get_nrics()

        if not nrics or len(nrics) == 0:
            log.CRITICAL("No more entries to process")
            raise CloseSpider("No more entries to process")

        form_data = {
            'CaptchaBox': str(captcha_string),
            'SEMAK': 'SEMAK',
        }
        for i, nric in enumerate(nrics):
            form_data.update({"{}{}".format(field_prefix, i + 1): nric})

        return FormRequest.from_response(
            response,
            formdata=form_data,
            callback=self.parse_result
        )

    def parse_result(self, response):
        # everything but the last row
        for result in response.selector.xpath(self.result_table_row)[:-1]:
            voter_item = VoterItem()
            voter_item['statename'] = self.statename
            loader = ItemLoader(voter_item, selector=result)
            loader.default_input_processor = MapCompose(unicode.strip)
            loader.default_output_processor = Join()

            for field, xpath in VoterItem_Fields.iteritems():
                loader.add_xpath(field, xpath)

            yield loader.load_item()

        self.restart()

    def restart(self):
        self.restarts += 1
        if self.restarts < MAX_RESTARTS:
            self.fields_counter = 3
            return Request(url=self.start_urls[0])

    def get_nrics(self):
        nrics = []
        session = DBSession()
        try:
            assert self.statename is not None

            # get all localities
            if not self.identify_locality:
                localities = session.query(Locality.id) \
                    .filter(Locality.statename == self.statename,
                            (Locality.flags.op('&')(Locality.FLAGS_EC_SPIDER_COMPLETED)) == 0) \
                    .all()
            else:
                localities = session.query(Locality.id) \
                    .filter(Locality.statename == self.statename,
                            ~Locality.resolved,
                            (Locality.flags.op('&')(Locality.FLAGS_EC_SPIDER_COMPLETED)) == 0) \
                    .all()

            localities_array = map(itemgetter(0), localities)

            # exclude localities that have been resolved or have a long enough address length
            resolved_query = session.query(Voter2.locality_id, func.count(Voter2.locality_id))

            if self.statename:
                resolved_query = resolved_query.filter(Voter2.statename == self.statename)

            voters2_localities_total = resolved_query \
                .filter(Voter2.resolved |
                        (func.character_length(Voter2.address1 + Voter2.address2 + Voter2.address3) > 10),
                        Voter2.locality_id.in_(localities_array)) \
                .group_by(Voter2.locality_id) \
                .all()

            voters2_voters_per_locality = {x[0]: x[1] for x in voters2_localities_total}

            for locale_id in localities_array:
                total_to_obtain = int(self.limit) - voters2_voters_per_locality.get(locale_id, 0)
                if total_to_obtain <= 0:
                    # mark this locality as processed
                    locality_row = session.query(Locality).filter(Locality.id == locale_id).first()
                    locality_row.flags |= Locality.FLAGS_EC_SPIDER_COMPLETED
                    session.commit()
                    continue

                nric_query = session.query(Voter.IC) \
                    .filter(Voter.Address1.is_(None), Voter.Kodlokaliti == locale_id,
                            Voter.has_nric) \
                    .order_by(Voter.IC.desc()) \
                    .limit(3)

                results = nric_query.all()

                # not enough voters to reach target limit so we mark it as completed
                if results.__len__() == 0:
                    locality_row = session.query(Locality).filter(Locality.id == locale_id).first()
                    locality_row.flags |= Locality.FLAGS_EC_SPIDER_COMPLETED
                    session.commit()

                for row in results:
                    nrics.append(row[0])

                    if len(nrics) >= RESULT_ROWS:
                        break

                if len(nrics) >= RESULT_ROWS:
                    break
        except Exception, ex:
            log.ERROR(ex.message)
            raise
        finally:
            session.close()

        return nrics[:3]

    def get_nrics_2015(self):
        nrics = []
        pars = []
        dms = []
        try:
            session = DBSession()

            query = session.query(func.left(Sarawak2014q1.Kodlokaliti, 3))

            if self.statename:
                query = query.filter(Sarawak2014q1.statename == self.statename)
                pars = map(itemgetter(0), query.group_by(func.left(Sarawak2014q1.Kodlokaliti, 3)).all())

            if self.par:
                pars.append(self.par)

            dms_query = session.query(Dm.kodlama)

            if len(pars) != 0:
                dms_query = dms_query.filter(Dm.parlama.in_(pars))
                dms = map(itemgetter(0), dms_query.order_by(Dm.kodlama).all())

            if self.dm:
                dms.append(self.dm)

            # get all localities
            if not self.identify_locality:
                localities = session.query(Locality.id).filter(Locality.dm_id.in_(dms)) \
                    .order_by(Locality.id).all()
            else:
                localities = session.query(Locality.id).filter(Locality.dm_id.in_(dms), ~Locality.resolved) \
                    .order_by(Locality.id).all()

            localities = map(itemgetter(0), localities)

            # exclude localities that have been resolved or have a long enough address length
            resolved_query = session.query(Sarawak2014q1Resolved.locality_id,
                                           func.count(Sarawak2014q1Resolved.locality_id))

            if self.statename:
                resolved_query = resolved_query.filter(Sarawak2014q1Resolved.statename == self.statename)

            result_resolved = resolved_query \
                .filter(Sarawak2014q1Resolved.resolved |
                        (func.character_length(Sarawak2014q1Resolved.address1 +
                                               Sarawak2014q1Resolved.address2 +
                                               Sarawak2014q1Resolved.address3) > 10),
                        Sarawak2014q1Resolved.locality_id.in_(localities)) \
                .group_by(Sarawak2014q1Resolved.locality_id) \
                .order_by(Sarawak2014q1Resolved.locality_id) \
                .all()

            resolvedmap = {x[0]: x[1] for x in result_resolved}

            for locale in localities:
                total_to_obtain = int(self.limit) - resolvedmap.get(locale, 0)
                if total_to_obtain <= 0:
                    continue

                results = session.query(Sarawak2014q1.IC) \
                    .filter(Sarawak2014q1.Address1.is_(None), Sarawak2014q1.Kodlokaliti == locale,
                            func.left(Sarawak2014q1.IC, 1).op('~')(r'\d')) \
                    .order_by(Sarawak2014q1.IC.desc()) \
                    .limit(total_to_obtain) \
                    .all()

                for row in results:
                    nrics.append(row[0])

                    if len(nrics) >= RESULT_ROWS:
                        break

                if len(nrics) >= RESULT_ROWS:
                    break
        except Exception, ex:
            raise
        finally:
            session.close()

        return nrics[:3]


"""
    def get_nrics(self):
        nrics = []
        pars = []
        dms = []
        try:
            if not self.session:
                self.session = DBSession()

            query = self.session.query(func.left(Voter.Kodlokaliti, 3))

            if self.statename:
                query = query.filter(Voter.statename == self.statename)
                pars = map(itemgetter(0), query.group_by(func.left(Voter.Kodlokaliti, 3)).all())

            if self.par:
                pars.append(self.par)

            dms_query = self.session.query(Dm.kodlama)

            if len(pars) != 0:
                dms_query = dms_query.filter(Dm.parlama.in_(pars))
                dms = map(itemgetter(0), dms_query.order_by(Dm.kodlama).all())

            if self.dm:
                dms.append(self.dm)

            # get all localities
            if not self.identify_locality:
                localities = self.session.query(Locality.id).filter(Locality.dm_id.in_(dms)) \
                    .order_by(Locality.id).all()
            else:
                localities = self.session.query(Locality.id).filter(Locality.dm_id.in_(dms), ~Locality.resolved) \
                    .order_by(Locality.id).all()

            localities = map(itemgetter(0), localities)

            # exclude localities that have been resolved or have a long enough address length
            resolved_query = self.session.query(Voter2.locality_id, func.count(Voter2.locality_id))

            if self.statename:
                resolved_query = resolved_query.filter(Voter2.statename == self.statename)

            result_resolved = resolved_query \
                .filter(
                Voter2.resolved | (func.character_length(Voter2.address1 + Voter2.address2 + Voter2.address3) > 10),
                Voter2.locality_id.in_(localities)) \
                .group_by(Voter2.locality_id) \
                .order_by(Voter2.locality_id) \
                .all()

            resolvedmap = {x[0]: x[1] for x in result_resolved}

            for locale in localities:
                total_to_obtain = int(self.limit) - resolvedmap.get(locale, 0)
                if total_to_obtain <= 0:
                    continue

                locality_query = self.session.query(Voter.IC) \
                    .filter(Voter.Address1.is_(None), Voter.Kodlokaliti == locale,
                            func.left(Voter.IC, 1).op('~')(r'\d')) \
                    .order_by(Voter.IC.desc()) \
                    .limit(total_to_obtain)

                results = locality_query.all()

                for row in results:
                    nrics.append(row[0])

                    if len(nrics) >= RESULT_ROWS:
                        break

                if len(nrics) >= RESULT_ROWS:
                    break
        except Exception, ex:
            log.ERROR(ex.message)
            raise
        finally:
            self.session.close()

        return nrics[:3]
"""
