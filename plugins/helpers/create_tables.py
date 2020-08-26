class sqlCreateTables:
    create_dim_Airport = """CREATE TABLE IF NOT EXISTS  public.dim_airports
    (
         iata     varchar  ,
         airport  varchar  NULL,
         city     varchar  NULL,
         country  varchar  NULL,
         wether   decimal(6,3)  NULL
    ); 
    """
    create_dim_carriers = """ CREATE TABLE IF NOT EXISTS  public.dim_carriers
    (
         code        varchar ,
         Description varchar NULL,
         tail_number varchar NULL
    );
    """
    create_dim_weather = """ CREATE TABLE IF NOT EXISTS  public.dim_weather
    (
        date                          date,
        AverageTemperature            decimal(6,3)  NULL,
        AverageTemperatureUncertainty decimal(6,3)  NULL
    );
    """
    create_fact_flights = """CREATE TABLE IF NOT EXISTS  public.fact_flights
    (
        flight_id     BIGINT  PRIMARY KEY,
        airline       varchar NULL,
        DepTime       decimal(10,3) NULL,
        ArrTime       decimal(10,3) NULL,
        FlightNum     varchar NULL,
        UniqueCarrier varchar NULL,
        orgin         varchar NULL,
        Dest          varchar NULL,
        dt            date NULL
    );
    """