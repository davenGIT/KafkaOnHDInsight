#!/bin/bash

re='^[0-9]+$'

if [ "$1" == "" ] || [ $# -gt 1 ]; then
    python3 producer.py -t 15 -r 0.2 -s mass &
    python3 producer.py -t 15 -s rotation &
    python3 producer.py -t 15 -r 10 -s FillPressure &
    python3 producer.py -t 15 -r 10 -s BowlTemp &
    python3 producer.py -t 15 -s OvenSpeed &
    python3 producer.py -t 15 -r 1 -s ProveTemp &
    python3 producer.py -t 15 -r 5 -s OvenTemp1 &
    python3 producer.py -t 15 -r 5 -s OvenTemp2 &
    python3 producer.py -t 15 -r 10 -s OvenTemp3 &
    python3 producer.py -t 15 -r 2 -s CoolTemp1 &
    python3 producer.py -t 15 -r 1 -s CoolTemp2 &
    python3 producer.py -t 15 -s PackSpeed &
    python3 producer.py -t 15 -r 10 -s PackCounter &
else
    if ! [[ $1 =~ $re ]] ; then
        echo "ermmmmror: Not a positive integer" >&2; exit 1
    else

        n=$(echo $1 | sed 's/^0*//')

		if (( n > 0 ));
		then
			python3 producer.py -t ${n} -r 0.2 -s mass &
			python3 producer.py -t ${n} -s rotation &
			python3 producer.py -t ${n} -r 10 -s FillPressure &
			python3 producer.py -t ${n} -r 10 -s BowlTemp &
			python3 producer.py -t ${n} -s OvenSpeed &
			python3 producer.py -t ${n} -r 1 -s ProveTemp &
			python3 producer.py -t ${n} -r 5 -s OvenTemp1 &
			python3 producer.py -t ${n} -r 5 -s OvenTemp2 &
			python3 producer.py -t ${n} -r 10 -s OvenTemp3 &
			python3 producer.py -t ${n} -r 2 -s CoolTemp1 &
			python3 producer.py -t ${n} -r 1 -s CoolTemp2 &
			python3 producer.py -t ${n} -s PackSpeed &
			python3 producer.py -t ${n} -r 10 -s PackCounter &
		else
			echo "error: Not a positive integer" >&2; exit 1
		fi
	fi
fi
echo Done!			 