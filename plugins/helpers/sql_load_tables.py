class sqlLoadingQuery:

    FLIGHT_FACT_INSERT = """
        SELECT flight_id, airline, DepTime, ArrTime, FlightNum, UniqueCarrier, orgin, Dest, dt
        FROM
        (SELECT flight_id, Description as airline , DepTime, ArrTime, FlightNum, UniqueCarrier, a.city as orgin,r.city as Dest, TO_DATE( Year || '-' || Month || '-' || DayofMonth,'YYYY-MM-DD') as dt  
        FROM public.flights_staging_table f 
        JOIN public.carriers_staging_table c on c.code = f.UniqueCarrier 
        JOIN public.airport_staging_table a on a.iata= f.orgin 
        JOIN public.airport_staging_table r on r.iata = f.Dest)  
        ORDER BY flight_id  
        
    """

    WEATHER_DIM_INSERT = """
        SELECT   dt as date, averagetemperature, averagetemperatureuncertainty
        FROM public.weather_staging_table 
        WHERE dt IN (SELECT  TO_DATE( Year || '-' || Month || '-' || DayofMonth,'YYYY-MM-DD'  ) AS dt
        FROM public.flights_staging_table )
     
    """
    
    AIRPORT_DIM_INSERT = """
            SELECT iata, airport, a.city as city , a.country as country, averagetemperature as wether
            FROM public.airport_staging_table  a 
            JOIN public.weather_staging_table w  on w.city = a.city
        
        """

    CARRIERS_DIM_INSERT = """ 
            SELECT code, Description, tailnum as tail_number 
            FROM public.flights_staging_table f
            JOIN public.carriers_staging_table c 
            ON c.code = f. uniquecarrier
       
       """
  
