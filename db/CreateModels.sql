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
 *  Finacial Tracking
 */

CREATE TYPE TemporalFrequency AS ENUM (
    'MONTHLY',
    'QUARTERLY',
    'ANNUALLY'
);

/*
 *  === Finacial Factor ===
 */
CREATE TABLE FinacialFactor(
    FactorID        SERIAL              PRIMARY KEY,
    FiscalValue     BIGINT              NOT NULL,
    ReportingFreq   TemporalFrequency   NOT NULL,
    Indicator       VARCHAR(255)        NOT NULL,
    Date            DATE                NOT NULL,

    TableID         INTEGER
        REFERENCES SourceTbl(TableID)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

COMMIT;
/* ROLLBACK; */
