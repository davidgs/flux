package environmental

import "math"


_steadman = (t,h) =>
    (0.5 * (t + 61.0 + ((t - 68.0)*1.2) + (h*0.094)))

_rothfusz = (t,h) => 
    -42.379 + 2.04901523*t + 10.14333127*h - .22475541*t*h - .00683783*t*t - .05481717*h*h + .00122874*t*t*h + .00085282*t*h*h - .00000199*t*t*h*h

_roth_adjust1 = (t,h) =>
    ((13.0-h)/4.0)*math.sqrt(x: ((17.0-math.abs(x: (t-95.0))/17.0)))
_roth_adjust2 = (t,h) =>
    ((h-85.0 )/10.0) *((87.0-t)/5.0)

heatIndex = (tables=<-) => tables
    |> map(fn: (r) => ({
        r with _value:
            if  (_steadman(t: r.temperature, h: r.humidity) + r.temperature)/2.0 < 80.0  then _steadman(t: r.temperature, h: r.humidity)
            else if ( r.humidty < 13.0 and r.temperature > 80.0) then _rothfusz(t: r.temperature, h: r.humidity) - _roth_adjust1(t: r.temperature, h: r.humidity)
            else if r.humidity > 85.0 and r.temperature >= 80.0 and r.temperature <= 87.0 then _rothfusz(t: r.temperature, h: r.humidity) + _roth_adjust2(t: r.temperature, h: r.humidity)
            else _rothfusz(t: r.temperature, h: r.humidity)
    }))


    
idealGasLaw = (tables=<-) => tables 
  |> map(fn: (r) => ({_time: r._time, _value: r.gas * (((r.temperature + 273.15) * 1013.25) / (r.pressure * 298.15))}))