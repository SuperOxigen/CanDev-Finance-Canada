/*
 *  CanDev Finance Canada - SQL - Drop Table Model
 *
 *  Removes the first version of the DB.
 *
 *  Copyright (c) 2018 Alex Dale
 *  See LICENSE for information.
 */

BEGIN;

DROP TABLE FinancialFactor;
DROP TYPE TemporalFrequency;

DROP TABLE StatsCanTbl;
DROP TABLE SourceTbl;
DROP TYPE SourceTblType;

/* ROLLBACK; */
COMMIT;
