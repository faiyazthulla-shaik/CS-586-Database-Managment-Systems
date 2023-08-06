import psycopg2
from sqlalchemy import create_engine
import os


def create_connection():
    connection = psycopg2.connect(
        host=os.environ['CLASS_DB_HOST'],
        database=os.environ['CLASS_DB_USERNAME'],
        user=os.environ['CLASS_DB_USERNAME'],
        password=os.environ['CLASS_DB_PASSWORD']
    )
    connection.autocommit = True
    return connection

def create_WDICountry(db_conn):
	cur=db_conn.cursor()
	create_stmt="CREATE TABLE WDICountry("\
			"CountryCode char(3) NOT NULL,"\
			"ShortName VARCHAR NOT NULL,"\
			"TableName VARCHAR NOT NULL,"\
			"LongName VARCHAR NOT NULL,"\
			"alpha2code char(2) NOT NULL,"\
			"CurrencyUnit VARCHAR,"\
			"SpecialNotes TEXT,"\
			"Region VARCHAR,"\
			"IncomeGroup VARCHAR,"\
			"WB2code char(2) NOT NULL,"\
			"National_accounts_base_year VARCHAR,"\
			"National_accounts_reference_year VARCHAR,"\
			"SNA_price_valuation VARCHAR,"\
			"Lending_category VARCHAR,"\
			"Other_groups VARCHAR,"\
			"System_of_National_Accounts VARCHAR,"\
			"Alternative_conversion_factor VARCHAR,"\
			"PPP_survey_year VARCHAR,"\
			"Balance_of_Payments_Manual_in_use VARCHAR,"\
			"External_debt_Reporting_status VARCHAR,"\
			"System_of_trade VARCHAR,"\
			"Government_Accounting_concept VARCHAR,"\
			"IMF_data_dissemination_standard VARCHAR,"\
			"Latest_population_census VARCHAR,"\
			"Latest_household_survey VARCHAR,"\
			"Source_of_most_recent_Income_and_expenditure_data VARCHAR,"\
			"Vital_registration_complete VARCHAR,"\
			"Latest_agricultural_census VARCHAR,"\
			"Latest_industrial_data int,"\
			"Latest_trade_data int,"\
			"PRIMARY KEY (CountryCode));"
	cur.execute(create_stmt)

def create_WDISeries(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE WDISeries(" \
                  "  SeriesCode VARCHAR NOT NULL," \
			"  Topic VARCHAR NOT NULL,"\
			"  IndicatorName VARCHAR NOT NULL,"\
			"  Shortdefinition TEXT,"\
			"  Longdefinition TEXT,"\
			"  Unit_of_measure VARCHAR,"\
			"  Periodicity VARCHAR,"\
			"  BasePeriod VARCHAR,"\
			"  Othernotes TEXT,"\
			"  Aggregation_method VARCHAR,"\
			"  Limitations_exceptions TEXT,"\
			"  Notes_original_source TEXT,"\
			"  General_comments TEXT,"\
			"  Source VARCHAR,"\
			"  Statistical_concept_methodology TEXT,"\
			"  Development_relevance TEXT,"\
			"  Related_source_links VARCHAR,"\
			"  Other_web_links VARCHAR,"\
			"  Related_indicators VARCHAR,"\
			"  LicenseType VARCHAR,"\
			"  PRIMARY KEY (SeriesCode)); "
    cur.execute(create_stmt)


def create_WDICountry_Series(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE WDICountry_Series(" \
                  "  CountryCode CHAR(3) NOT NULL," \
                  "  SeriesCode VARCHAR NOT NULL," \
                  "  Description TEXT NOT NULL," \
			"  FOREIGN KEY (CountryCode) REFERENCES WDICountry(CountryCode),"\
			"  FOREIGN KEY (SeriesCode) REFERENCES WDISeries(SeriesCode));"
    cur.execute(create_stmt)

def create_WDIData(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE WDIData(" \
                  "  CountryName VARCHAR NOT NULL," \
                  "  CountryCode CHAR(3) NOT NULL," \
                  "  IndicatorName VARCHAR NOT NULL," \
			"  IndicatorCode VARCHAR NOT NULL,"\
			"  Y1960 numeric(20,5),"\
			"  Y1961 numeric(20,5),"\
			"  Y1962 numeric(20,5),"\
			"  Y1963 numeric(20,5),"\
			"  Y1964 numeric(20,5),"\
			"  Y1965 numeric(20,5),"\
			"  Y1966 numeric(20,5),"\
			"  Y1967 numeric(20,5),"\
			"  Y1968 numeric(20,5),"\
			"  Y1969 numeric(20,5),"\
			"  Y1970 numeric(20,5),"\
			"  Y1971 numeric(20,5),"\
			"  Y1972 numeric(20,5),"\
			"  Y1973 numeric(20,5),"\
			"  Y1974 numeric(20,5),"\
			"  Y1975 numeric(20,5),"\
			"  Y1976 numeric(20,5),"\
			"  Y1977 numeric(20,5),"\
			"  Y1978 numeric(20,5),"\
			"  Y1979 numeric(20,5),"\
			"  Y1980 numeric(20,5),"\
			"  Y1981 numeric(20,5),"\
			"  Y1982 numeric(20,5),"\
			"  Y1983 numeric(20,5),"\
			"  Y1984 numeric(20,5),"\
			"  Y1985 numeric(20,5),"\
			"  Y1986 numeric(20,5),"\
			"  Y1987 numeric(20,5),"\
			"  Y1988 numeric(20,5),"\
			"  Y1989 numeric(20,5),"\
			"  Y1990 numeric(20,5),"\
			"  Y1991 numeric(20,5),"\
			"  Y1992 numeric(20,5),"\
			"  Y1993 numeric(20,5),"\
			"  Y1994 numeric(20,5),"\
			"  Y1995 numeric(20,5),"\
			"  Y1996 numeric(20,5),"\
			"  Y1997 numeric(20,5),"\
			"  Y1998 numeric(20,5),"\
			"  Y1999 numeric(20,5),"\
			"  Y2000 numeric(20,5),"\
			"  Y2001 numeric(20,5),"\
			"  Y2002 numeric(20,5),"\
			"  Y2003 numeric(20,5),"\
			"  Y2004 numeric(20,5),"\
			"  Y2005 numeric(20,5),"\
			"  Y2006 numeric(20,5),"\
			"  Y2007 numeric(20,5),"\
			"  Y2008 numeric(20,5),"\
			"  Y2009 numeric(20,5),"\
			"  Y2010 numeric(20,5),"\
			"  Y2011 numeric(20,5),"\
			"  Y2012 numeric(20,5),"\
			"  Y2013 numeric(20,5),"\
			"  Y2014 numeric(20,5),"\
			"  Y2015 numeric(20,5),"\
			"  Y2016 numeric(20,5),"\
			"  Y2017 numeric(20,5),"\
			"  Y2018 numeric(20,5),"\
			"  Y2019 numeric(20,5),"\
			"  Y2020 numeric(20,5),"\
			"  Y2021 numeric(20,5),"\
			"  PRIMARY KEY (CountryCode, IndicatorCode),"\
			"  FOREIGN KEY (CountryCode) REFERENCES WDICountry(CountryCode));"
    cur.execute(create_stmt)


def create_WDIFootNote(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE WDIFootNote(" \
                  "  CountryCode CHAR(3) NOT NULL," \
                  "  SeriesCode VARCHAR NOT NULL," \
			"  Year VARCHAR NOT NULL,"\
                  "  Description TEXT NOT NULL," \
			"  FOREIGN KEY (CountryCode) REFERENCES WDICountry(CountryCode),"\
			"  FOREIGN KEY (SeriesCode) REFERENCES WDISeries(SeriesCode));"
    cur.execute(create_stmt)


def create_WDISeries_Time(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE WDISeries_Time(" \
                  "  SeriesCode VARCHAR NOT NULL," \
			"  Year VARCHAR NOT NULL,"\
                  "  Description TEXT NOT NULL," \
			"  FOREIGN KEY (SeriesCode)REFERENCES WDISeries(SeriesCode));"
    cur.execute(create_stmt)


def insert_df_rows_to_table(df, table_name):
    engine = create_engine(
        f"postgresql://{os.environ['CLASS_DB_USERNAME']}:"
        f"{os.environ['CLASS_DB_PASSWORD']}@"
        f"{os.environ['CLASS_DB_HOST']}:"
        f"5432/{os.environ['CLASS_DB_USERNAME']}"
    )
    df.to_sql(table_name, engine, if_exists="append", index=False)


if __name__ == "__main__":
	conn = create_connection()
	create_WDIData(conn)