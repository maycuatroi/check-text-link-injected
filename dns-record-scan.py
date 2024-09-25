import dns.resolver
import sys


def scan_dns_records(domain):
    record_types = ["A", "AAAA", "MX", "NS", "TXT", "CNAME"]

    print(f"DNS records for {domain}:\n")

    for record_type in record_types:
        try:
            answers = dns.resolver.resolve(domain, record_type)
            print(f"{record_type} records:")
            for rdata in answers:
                print(f"  {rdata}")
        except dns.resolver.NoAnswer:
            print(f"No {record_type} records found")
        except dns.resolver.NXDOMAIN:
            print(f"The domain {domain} does not exist")
            return
        except dns.exception.DNSException as e:
            print(f"An error occurred while querying {record_type} records: {e}")
        print()


if __name__ == "__main__":
    domain = "hondaotovovankiet.vn"
    scan_dns_records(domain)
