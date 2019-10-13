
# Rippler-flight-analytics

A web-app for flight delay visualization with all domestic flights in United States
Using Python Flask. Deployed with gunicorn and nginx. 

Model trained using PySpark Machine learning library

# Table of Contents
1. [Flight delay ripple effect](README.md#Flight-delay-ripple-effect)
2. [Dataset](README.md#Dataset)
3. [Architecture](README.md#Architecture)
4. [Web App](README.md#Web-App)

## Flight delay ripple effect

Flight delays and cancellations happen every single day at airports. The causes for such are usually minor, but every now and then some unforeseen variables can cause major disruptions nationwide. Considering inclement weather, electrical problems, transportation issues and the occasional outliers like malfunctioning plumbing, at what point do these variables surpass a threshold that impacts a significant number of travelers? And to what extent do the trickle effects cause serious problems beyond the date of impact?
In particular, the delay has the tendecy to propagate and affect other flight schedules.
At what point cancelling or delaying a flight further, would mitigate the delay propagation. The motive is to predict the likely hood of cancellation and estimating expected delays in a flight

## Dataset

Historical Weather data
Historical Airlines delay data

## Architecture
S3->Spark->Postgresql->flask

## Web App
Try it out!
http://www.flightrippler.com/


