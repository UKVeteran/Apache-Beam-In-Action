import logging
import requests
from datetime import datetime
import argparse

import apache_beam as beam


def parse_lines(element):
    return element.split(",")


class CalcVisitDuration(beam.DoFn):
    def process(self, element):
        dt_format = "%Y-%m-%dT%H:%M:%S"
        start_dt = datetime.strptime(element[1], dt_format)
        end_dt = datetime.strptime(element[2], dt_format)

        diff = end_dt - start_dt

        yield [element[0], diff.total_seconds()]


class GetIpCountryOrigin(beam.DoFn):
    def process(self, element):
        ip=element[0]
        response=requests.get(f"http://ip-api.com/json/{ip}?fields=country")
        country=response.json()["country"]


        yield[ip, country]







def run(argv=None):
    parser=argparse.ArgumentParser()
    parser.add_argument("--input")
    parser.add_argument("--output")
    args, beam_args=parser.parse_known_args(argv)

    with beam.Pipeline() as p:
        lines= (
            p
            | "ReadFile" >> beam.io.ReadFromText(args.input,skip_header_lines=1)
            |  "ParseLines">>beam.Map(parse_lines)
            )

        duration =  lines | "CalcVisitDuration">> beam.ParDo(CalcVisitDuration())

        ip_map = lines | "GetIpCountryOrigin">> beam.ParDo(GetIpCountryOrigin())

        ip_map | beam.Map(print)







if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARNING)
    run()
