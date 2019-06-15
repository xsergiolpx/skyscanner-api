# skyscanner-api

Using the Skyscanner website to find good flight deals when you have flexibility in the destinations and dates can take a long time because:
* As we add potential airport and combinations of dates these combinations increase polynomially
* The web interface in the monthly view is not necessarily updated, which means that there are deals that can be hidden
* Prices change daily, so an automatic search is needed

This software uses the Skyscanner public API to query the combination of flights with the following parameters:
* Origin airport
* Destination airports
* Date limits
* Minimum and maximum number of days for the trip

It returns and saves dataframes with the following information:
* Flights durations
* Airport layovers
* Price
* And many more!