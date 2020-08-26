class sqlCreateAndQuery:
    CREATE_CARRIERS_STAGING_TABLE= """ CREATE TABLE IF NOT EXISTS public.carriers_staging_table
    (  
       id           bigint NULL,
       code         VARCHAR NULL,
       Description  VARCHAR NULL 
    );
    """
    
    CREATE_FLIGHTS_STAGING_TABLE= """ CREATE TABLE IF NOT EXISTS public.flights_staging_table
    (  
      flight_id      bigint NULL,  
      Year           integer NULL,
      Month          integer NULL,
      DayofMonth     integer NULL,
      DayOfWeek      integer NULL,
      DepTime        DECIMAL(10,3) NULL,
      ArrTime        DECIMAL(10,3)  NULL,
      UniqueCarrier  VARCHAR NULL, 
      TailNum        VARCHAR NULL, 
      FlightNum      integer  NULL,
      orgin          VARCHAR NULL,
      Dest           VARCHAR  NULL,
      Distance       NUMERIC  NULL
    );
    """
    
    CREATE_WEATHER_STAGING_TABLE = """ CREATE TABLE IF NOT EXISTS public.weather_staging_table
    (     dt                             date NULL,
          AverageTemperature             DECIMAL(6,3) NULL,
          AverageTemperatureUncertainty  DECIMAL(6,3) NULL,
          city                           varchar NULL,
          Country                        varchar NULL,
          Latitude                       varchar NULL,
          Longitude                      varchar NULL 
    );
    """
    
    CREATE_AIRPORT_STAGING_TABLE = """ CREATE TABLE IF NOT EXISTS public.airport_staging_table
    ( 
      id       bigint NULL,
      iata     varchar NULL,
      airport  varchar NULL,
      city     varchar NULL,
      state    varchar NULL,
      country  varchar NULL,
      lat      varchar NULL,
      long     varchar NULL
    );
    """