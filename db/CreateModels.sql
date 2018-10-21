/*
 *  CanDev Finance Canada - SQL - Create Table Model
 *
 *  Creates the first version of the DB.
 *
 *  Copyright (c) 2018 Alex Dale
 *  See LICENSE for information.
 */

BEGIN;

/*
 *  Table Source Tracking
 */

/*
 *  === SourceTblType ===
 *  The type of table which the data is from.
 *  Types:
 *      STATSCAN - information comes from Statistics Canada.
 */
CREATE TYPE SourceTblType AS ENUM (
    'UNKNOWN',
    'STATSCAN'
);

/*
 *  === SourceTbl ===
 *  Information about the source of an entry.
 *
 */
CREATE TABLE SourceTbl(
    TableID         SERIAL          PRIMARY KEY,
    Type            SourceTblType   NOT NULL,
    LastUpdate      DATE            NOT NULL
);

CREATE TABLE StatsCanTbl(
    TableID         INTEGER
        REFERENCES SourceTbl(TableID)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    StatsCanTableID INTEGER,
    PRIMARY KEY(TableID, StatsCanTableID)
);

/*
 *  Financial Tracking
 */

CREATE TYPE TemporalFrequency AS ENUM (
    'MONTHLY',
    'QUARTERLY',
    'ANNUALLY'
);

/*
 *  === Financial Factor ===
 */
CREATE TABLE FinancialFactor(
    FactorID        SERIAL              PRIMARY KEY,
    FiscalValue     BIGINT              NOT NULL,
    Frequency       TemporalFrequency   NOT NULL,
    Indicator       VARCHAR(255)        NOT NULL,
    MainCategory    VARCHAR(255)        NOT NULL,
    Date            DATE                NOT NULL,

    TableID         INTEGER
        REFERENCES SourceTbl(TableID)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

COMMIT;
/* ROLLBACK; */
