-- ============================================================
-- HeadHunter Vacancy Collector — SQL Server Schema v2
-- ============================================================

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'headhunter')
    CREATE DATABASE headhunter;
GO
USE headhunter;
GO

IF OBJECT_ID('companies', 'U') IS NULL
CREATE TABLE companies (
    id      INT           PRIMARY KEY,
    name    NVARCHAR(255) NOT NULL,
    website NVARCHAR(500)
);
GO

IF OBJECT_ID('locations', 'U') IS NULL
CREATE TABLE locations (
    id      INT           PRIMARY KEY,
    country NVARCHAR(100) NOT NULL DEFAULT 'Uzbekistan',
    city    NVARCHAR(100)
);
GO

IF OBJECT_ID('skills', 'U') IS NULL
CREATE TABLE skills (
    id   INT           PRIMARY KEY,
    name NVARCHAR(255) NOT NULL UNIQUE
);
GO

IF OBJECT_ID('vacancies', 'U') IS NULL
CREATE TABLE vacancies (
    h_id         BIGINT        PRIMARY KEY,
    title        NVARCHAR(500),
    position     NVARCHAR(255),
    category     NVARCHAR(255),
    publish_date DATE,
    company      NVARCHAR(255),
    skills       NVARCHAR(MAX),
    country      NVARCHAR(100),
    location     NVARCHAR(100),
    min_salary   DECIMAL(18,2),
    max_salary   DECIMAL(18,2),
    currency     NVARCHAR(10)
);
GO

CREATE INDEX IX_vac_date     ON vacancies(publish_date);
CREATE INDEX IX_vac_company  ON vacancies(company);
CREATE INDEX IX_vac_category ON vacancies(category);
GO

IF OBJECT_ID('vacancy_skill', 'U') IS NULL
CREATE TABLE vacancy_skill (
    h_id     BIGINT NOT NULL,
    skill_id INT    NOT NULL,
    PRIMARY KEY (h_id, skill_id),
    FOREIGN KEY (h_id)     REFERENCES vacancies(h_id),
    FOREIGN KEY (skill_id) REFERENCES skills(id)
);
GO
