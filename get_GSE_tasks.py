from datetime import datetime, timedelta
import pandas as pd
import numpy as np
# define a function to get GSE service start time, end time, energy consumption, and other specs
def get_gse_schedule(flight_schedule):
    df = flight_schedule.copy()
    df['flight_arr_time'] = df['Arrival_time']
    df = df.sort_values(by = ['flight_arr_time'])  # sort flight data by flight arrival time

    #vehicle_id = []
    airport = []
    air_line = []
    tail_number = []
    aircraft_type_list = []
    GSE_type = []
    energy_cons_rate = []
    batt_cap = []
    start_time = []
    end_time = []
    energy_consumption = []


    # GSE operating time for narrow-body aircraft (min)
    air_tractor_tim_n = 7   # aircraft tractor operating time in mode
    GPU_tim_n = 35          # GPU operating time in mode
    bag_tractor_tim_n = 28  # baggage tractor operating time in mode
    belt_loader_tim_n = 47  # belt loadr operating time in mode
    cater_truck_tim_n = 21  # catering truck operating time in mode
    lav_truck_tim_n = 8     # lavatory truck operating time in mode
    water_truck_tim_n = 5   # fuel truck operating time in mode

    # GSE rated power for narrow-body aircraft (kW)
    air_tractor_power_n = 69        # aircraft tractor rated power
    GPU_power_n = 90                # GPU rated power
    bag_tractor_power_n = 22        # baggage tractor rated power
    belt_loader_power_d_n = 3.7     # belt loadr rated power for driving
    belt_loader_power_b_n = 1.13    # belt loadr rated power for belt drive
    cater_truck_power_n = 100       # catering truck rated power
    lav_truck_power_n = 30          # lavatory truck rated power
    water_truck_power_n = 30        # fuel truck rated power

    # GSE battery capacity for narrow-body aircraft (kWh)
    air_tractor_batt_n = 72         # aircraft tractor battery capacity
    GPU_batt_n = 160                # GPU battery capacity
    bag_tractor_batt_n = 50         # baggage tractor battery capacity
    belt_loader_batt_n = 31.7       # belt loadr battery capacity
    cater_truck_batt_n = 246.67     # catering truck battery capacity
    lav_truck_batt_n = 40           # lavatory truck battery capacity
    water_truck_batt_n = 40         # fuel truck battery capacity

    # GSE operating time for wide-body aircraft (min)
    air_tractor_tim_w = 12      # aircraft tractor operating time in mode
    GPU_tim_w = 57              # GPU operating time in mode
    cargo_loader_tim_w = 50     # cargo loader operating time in mode
    bag_tractor_tim_w = 53      # baggage tractor operating time in mode
    belt_loader_tim_w = 42      # belt loadr operating time in mode
    cater_truck_tim_w = 28      # catering truck operating time in mode
    lav_truck_tim_w = 17        # lavatory truck operating time in mode
    water_truck_tim_w = 5       # fuel truck operating time in mode

    # GSE rated power for wide-body aircraft (kW)
    air_tractor_power_w = 103.5        # aircraft tractor rated power
    GPU_power_w = 180              # GPU rated power
    cargo_loader_power_d_w = 22        # cargo loader rated power for driving
    cargo_loader_power_l_w = 32        # cargo loader rated power for lifting
    bag_tractor_power_w = 22           # baggage tractor rated power
    belt_loader_power_d_w = 30         # belt loadr rated power for driving
    belt_loader_power_b_w = 30         # belt loadr rated power for belt drive
    cater_truck_power_w = 100          # catering truck rated power
    lav_truck_power_w = 60             # lavatory truck rated power
    water_truck_power_w = 100          # fuel truck rated power

    # GSE battery capacity for wide-body aircraft (kWh)
    air_tractor_batt_w = 168         # aircraft tractor battery capacity
    GPU_batt_w = 310                 # GPU battery capacity
    bag_tractor_batt_w = 50          # baggage tractor battery capacity
    belt_loader_batt_w = 34          # belt loadr battery capacity
    cater_truck_batt_w = 246.67      # catering truck battery capacity
    lav_truck_batt_w = 106.95        # lavatory truck battery capacity
    water_truck_batt_w = 106         # fuel truck battery capacity
    cargo_loader_batt_w = 90         # cargo loader battery capacity

    # GSE power load factor (for both narrow-body and wide-body aircraft)
    air_tractor_power_load = 0.8
    GPU_power_load = 0.75
    bag_tractor_power_load = 0.55
    cargo_loader_power_load = 0.5
    belt_loader_power_load = 0.5
    cater_truck_power_load = 0.53
    lav_truck_power_load = 0.25
    water_truck_power_load = 0.2
        
    # GSE energy consumption for servicing narrow-body aircraft (kWh)
    air_tractor_energy_n = (air_tractor_tim_n+10)/60*air_tractor_power_n*air_tractor_power_load         # 5 min driving to and 5 min driving from the gate for service
    bag_tractor_energy_n = (bag_tractor_tim_n+10)/60*bag_tractor_power_n*bag_tractor_power_load         # 5 min driving to and 5 min driving from the gate for service
    cater_truck_energy_n = (cater_truck_tim_n+10)/60*cater_truck_power_n*cater_truck_power_load         # 5 min driving to and 5 min driving from the gate for service
    GPU_energy_n = (GPU_tim_n+10)/60*GPU_power_n*GPU_power_load                                         # 5 min driving to and 5 min driving from the gate for service
    belt_loader_energy_n = (belt_loader_tim_n/60*belt_loader_power_b_n + 10/60*belt_loader_power_d_n)*belt_loader_power_load  # 5 min driving to and 5 min driving from the gate for service
    lav_truck_energy_n = (lav_truck_tim_n+10)/60*lav_truck_power_n*lav_truck_power_load                 # 5 min driving to and 5 min driving from the gate for service
    water_truck_energy_n = (water_truck_tim_n+10)/60*water_truck_power_n*water_truck_power_load         # 5 min driving to and 5 min driving from the gate for service

    # GSE energy consumption for servicing wide-body aircraft (kWh)
    air_tractor_energy_w = (air_tractor_tim_w+10)/60*air_tractor_power_w*air_tractor_power_load         # 5 min driving to and 5 min driving from the gate for service
    bag_tractor_energy_w = (bag_tractor_tim_w+10)/60*bag_tractor_power_w *bag_tractor_power_load        # 5 min driving to and 5 min driving from the gate for service
    cater_truck_energy_w = (cater_truck_tim_w+10)/60*cater_truck_power_w*cater_truck_power_load         # 5 min driving to and 5 min driving from the gate for service
    GPU_energy_w = (GPU_tim_w+10)/60*GPU_power_w*GPU_power_load                                         # 5 min driving to and 5 min driving from the gate for service
    belt_loader_energy_w = (belt_loader_tim_w/60*belt_loader_power_b_w + 10/60*belt_loader_power_d_w)*belt_loader_power_load  # 5 min driving to and 5 min driving from the gate for service
    lav_truck_energy_w = (lav_truck_tim_w+10)/60*lav_truck_power_w*lav_truck_power_load                 # 5 min driving to and 5 min driving from the gate for service
    water_truck_energy_w = (water_truck_tim_w+10)/60*water_truck_power_w*water_truck_power_load         # 5 min driving to and 5 min driving from the gate for service
    cargo_loader_energy_w = (cargo_loader_tim_w/60*cargo_loader_power_l_w + 10/60*cargo_loader_power_d_w)*cargo_loader_power_load  # 5 min driving to and 5 min driving from the gate for service

    # loop through each aircraft to get information and determine GSE usage
    for i in range(len(df)):
        arr_airport = df['Destination Airport'].iloc[i]
        airline = df['Carrier Code'].iloc[i] # which airline the aircraft belons to
        tailnumber = df['Tail Number'].iloc[i]
        arr_time = df['flight_arr_time'].iloc[i] # aircraft arrive time
        aircraft_type = df['aircraft_type'].iloc[i] # aircraft type

        air_tractor_start_time_n = arr_time + timedelta(minutes=45) # aircraft tractor service start time, aircraft tractor starts working when the aircraft departs
        air_tractor_start_time_w = arr_time + timedelta(minutes=90) # aircraft tractor service start time, aircraft tractor starts working when the aircraft departs
        GPU_start_time = arr_time + timedelta(minutes=5) # GPU service start time
        cargo_loader_start_time = arr_time + timedelta(minutes=5) # cargo loadr service start time
        bag_tractor_start_time = arr_time + timedelta(minutes=10) # baggage tractor service start time
        belt_loader_start_time = arr_time + timedelta(minutes=5) # belt loadr service start time
        cater_truck_start_time = arr_time + timedelta(minutes=5) # catering truck service start time
        lav_truck_start_time = arr_time - timedelta(minutes=5)    # lavatory truck service start time
        water_truck_start_time = arr_time - timedelta(minutes=5)  # fuel truck service start time

        air_tractor_end_time_n = air_tractor_start_time_n + timedelta(minutes=air_tractor_tim_n) + timedelta(minutes=10)# aircraft tractor service end time
        GPU_end_time_n = GPU_start_time + timedelta(minutes=GPU_tim_n) + timedelta(minutes=10)
        bag_tractor_end_time_n = bag_tractor_start_time +  timedelta(minutes=bag_tractor_tim_n) + timedelta(minutes=10) # baggage tractor service end time
        belt_loader_end_time_n = belt_loader_start_time +  timedelta(minutes=belt_loader_tim_n) + timedelta(minutes=10) # belt loadr service end time
        cater_truck_end_time_n = cater_truck_start_time +  timedelta(minutes=cater_truck_tim_n) + timedelta(minutes=10) # catering truck service end time
        lav_truck_end_time_n = lav_truck_start_time +  timedelta(minutes=lav_truck_tim_n) + timedelta(minutes=10) # lavatory truck service end time
        water_truck_end_time_n = water_truck_start_time + timedelta(minutes=water_truck_tim_n) + timedelta(minutes=10)# fuel truck service end time

        air_tractor_end_time_w = air_tractor_start_time_w + timedelta(minutes=air_tractor_tim_w) + timedelta(minutes=10)# aircraft tractor service end time
        GPU_end_time_w = GPU_start_time + timedelta(minutes=GPU_tim_w) + timedelta(minutes=10)
        cargo_loader_end_time_w = cargo_loader_start_time + timedelta(minutes=cargo_loader_tim_w) + timedelta(minutes=10)
        bag_tractor_end_time_w = bag_tractor_start_time +  timedelta(minutes=bag_tractor_tim_w) + timedelta(minutes=10) # baggage tractor service end time
        belt_loader_end_time_w = belt_loader_start_time +  timedelta(minutes=belt_loader_tim_w) + timedelta(minutes=10) # belt loadr service end time
        cater_truck_end_time_w = cater_truck_start_time +  timedelta(minutes=cater_truck_tim_w) + timedelta(minutes=10) # catering truck service end time
        lav_truck_end_time_w = lav_truck_start_time +  timedelta(minutes=lav_truck_tim_w) + timedelta(minutes=10) # lavatory truck service end time
        water_truck_end_time_w = water_truck_start_time + timedelta(minutes=water_truck_tim_w) + timedelta(minutes=10)# fuel truck service end time

        if aircraft_type == 'narrow':

            #aircraft tractor
            GSE_type.append('aircraft tractor')
            airport.append(arr_airport)
            air_line.append(airline)
            tail_number.append(tailnumber)
            energy_cons_rate.append(air_tractor_power_n) 
            batt_cap.append(air_tractor_batt_n)
            start_time.append(air_tractor_start_time_n) 
            end_time.append(air_tractor_end_time_n)
            energy_consumption.append(air_tractor_energy_n)
            aircraft_type_list.append(aircraft_type)

            #GPU
            GSE_type.append('GPU')
            airport.append(arr_airport)
            air_line.append(airline)
            tail_number.append(tailnumber)
            energy_cons_rate.append(GPU_power_n) 
            batt_cap.append(GPU_batt_n)
            start_time.append(GPU_start_time) 
            end_time.append(GPU_end_time_n)
            energy_consumption.append(GPU_energy_n)
            aircraft_type_list.append(aircraft_type)

            #assume 2 baggage tractors are needed to serve a narrow body aircraft
            GSE_type.append('baggage tractor')
            airport.append(arr_airport)
            air_line.append(airline)
            tail_number.append(tailnumber)
            energy_cons_rate.append(bag_tractor_power_n) 
            batt_cap.append(bag_tractor_batt_n)
            start_time.append(bag_tractor_start_time) 
            end_time.append(bag_tractor_end_time_n)
            energy_consumption.append(bag_tractor_energy_n)
            aircraft_type_list.append(aircraft_type)

            GSE_type.append('baggage tractor')
            airport.append(arr_airport)
            air_line.append(airline)
            tail_number.append(tailnumber)
            energy_cons_rate.append(bag_tractor_power_n) 
            batt_cap.append(bag_tractor_batt_n)
            start_time.append(bag_tractor_start_time) 
            end_time.append(bag_tractor_end_time_n)
            energy_consumption.append(bag_tractor_energy_n)
            aircraft_type_list.append(aircraft_type)

            #assume one catering truck for narrow-body aircraft
            GSE_type.append('catering truck')
            airport.append(arr_airport)
            air_line.append(airline)
            tail_number.append(tailnumber)
            energy_cons_rate.append(cater_truck_power_n) 
            batt_cap.append(cater_truck_batt_n)
            start_time.append(cater_truck_start_time) 
            end_time.append(cater_truck_end_time_n)
            energy_consumption.append(cater_truck_energy_n)
            aircraft_type_list.append(aircraft_type)

            #belt loader
            GSE_type.append('belt loader')
            airport.append(arr_airport)
            air_line.append(airline)
            tail_number.append(tailnumber)
            energy_cons_rate.append(belt_loader_power_b_n) 
            batt_cap.append(belt_loader_batt_n)
            start_time.append(belt_loader_start_time) 
            end_time.append(belt_loader_end_time_n)
            energy_consumption.append(belt_loader_energy_n)
            aircraft_type_list.append(aircraft_type)
            
            #lavatory truck
            GSE_type.append('lavatory truck')
            airport.append(arr_airport)
            air_line.append(airline)
            tail_number.append(tailnumber)
            energy_cons_rate.append(lav_truck_power_n) 
            batt_cap.append(lav_truck_batt_n)
            start_time.append(lav_truck_start_time) 
            end_time.append(lav_truck_end_time_n)
            energy_consumption.append(lav_truck_energy_n)
            aircraft_type_list.append(aircraft_type)

            #water truck
            GSE_type.append('water truck')
            airport.append(arr_airport)
            air_line.append(airline)
            tail_number.append(tailnumber)
            energy_cons_rate.append(water_truck_power_n) 
            batt_cap.append(water_truck_batt_n)
            start_time.append(water_truck_start_time) 
            end_time.append(water_truck_end_time_n)
            energy_consumption.append(water_truck_energy_n)
            aircraft_type_list.append(aircraft_type)

        else: # wide-body aircraft
        
            #aircraft tractor
            GSE_type.append('aircraft tractor')
            airport.append(arr_airport)
            air_line.append(airline)
            tail_number.append(tailnumber)
            energy_cons_rate.append(air_tractor_power_w) 
            batt_cap.append(air_tractor_batt_w)
            start_time.append(air_tractor_start_time_w) 
            end_time.append(air_tractor_end_time_w)
            energy_consumption.append(air_tractor_energy_w)
            aircraft_type_list.append(aircraft_type)

            #GPU
            GSE_type.append('GPU')
            airport.append(arr_airport)
            air_line.append(airline)
            tail_number.append(tailnumber)
            energy_cons_rate.append(GPU_power_w) 
            batt_cap.append(GPU_batt_w)
            start_time.append(GPU_start_time) 
            end_time.append(GPU_end_time_w)
            energy_consumption.append(GPU_energy_w)
            aircraft_type_list.append(aircraft_type)

            #assume 3 baggage tractors are needed to serve a wide body aircraft
            GSE_type.append('baggage tractor')
            airport.append(arr_airport)
            air_line.append(airline)
            tail_number.append(tailnumber)
            energy_cons_rate.append(bag_tractor_power_w) 
            batt_cap.append(bag_tractor_batt_w)
            start_time.append(bag_tractor_start_time) 
            end_time.append(bag_tractor_end_time_w)
            energy_consumption.append(bag_tractor_energy_w)
            aircraft_type_list.append(aircraft_type)

            GSE_type.append('baggage tractor')
            airport.append(arr_airport)
            air_line.append(airline)
            tail_number.append(tailnumber)
            energy_cons_rate.append(bag_tractor_power_w) 
            batt_cap.append(bag_tractor_batt_w)
            start_time.append(bag_tractor_start_time) 
            end_time.append(bag_tractor_end_time_w)
            energy_consumption.append(bag_tractor_energy_w)
            aircraft_type_list.append(aircraft_type)

            GSE_type.append('baggage tractor')
            airport.append(arr_airport)
            air_line.append(airline)
            tail_number.append(tailnumber)
            energy_cons_rate.append(bag_tractor_power_w) 
            batt_cap.append(bag_tractor_batt_w)
            start_time.append(bag_tractor_start_time) 
            end_time.append(bag_tractor_end_time_w)
            energy_consumption.append(bag_tractor_energy_w)
            aircraft_type_list.append(aircraft_type)

            #assume two catering truck for wide-body aircraft
            GSE_type.append('catering truck')
            airport.append(arr_airport)
            air_line.append(airline)
            tail_number.append(tailnumber)
            energy_cons_rate.append(cater_truck_power_w) 
            batt_cap.append(cater_truck_batt_w)
            start_time.append(cater_truck_start_time) 
            end_time.append(cater_truck_end_time_w)
            energy_consumption.append(cater_truck_energy_w)
            aircraft_type_list.append(aircraft_type)

            GSE_type.append('catering truck')
            airport.append(arr_airport)
            air_line.append(airline)
            tail_number.append(tailnumber)
            energy_cons_rate.append(cater_truck_power_w) 
            batt_cap.append(cater_truck_batt_w)
            start_time.append(cater_truck_start_time) 
            end_time.append(cater_truck_end_time_w)
            energy_consumption.append(cater_truck_energy_w)
            aircraft_type_list.append(aircraft_type)

            #belt loader
            GSE_type.append('belt loader')
            airport.append(arr_airport)
            air_line.append(airline)
            tail_number.append(tailnumber)
            energy_cons_rate.append(belt_loader_power_d_w) 
            batt_cap.append(belt_loader_batt_w)
            start_time.append(belt_loader_start_time) 
            end_time.append(belt_loader_end_time_w)
            energy_consumption.append(belt_loader_energy_w)
            aircraft_type_list.append(aircraft_type)
            
            #lavatory truck
            GSE_type.append('lavatory truck')
            airport.append(arr_airport)
            air_line.append(airline)
            tail_number.append(tailnumber)
            energy_cons_rate.append(lav_truck_power_w) 
            batt_cap.append(lav_truck_batt_w)
            start_time.append(lav_truck_start_time) 
            end_time.append(lav_truck_end_time_w)
            energy_consumption.append(lav_truck_energy_w)
            aircraft_type_list.append(aircraft_type)

            #water truck
            GSE_type.append('water truck')
            airport.append(arr_airport)
            air_line.append(airline)
            tail_number.append(tailnumber)
            energy_cons_rate.append(water_truck_power_w) 
            batt_cap.append(water_truck_batt_w)
            start_time.append(water_truck_start_time) 
            end_time.append(water_truck_end_time_w)
            energy_consumption.append(water_truck_energy_w)
            aircraft_type_list.append(aircraft_type)

            # 2 cargo loaders
            GSE_type.append('cargo loader')
            airport.append(arr_airport)
            air_line.append(airline)
            tail_number.append(tailnumber)
            energy_cons_rate.append(cargo_loader_power_l_w) 
            batt_cap.append(cargo_loader_batt_w)
            start_time.append(cargo_loader_start_time) 
            end_time.append(cargo_loader_end_time_w)
            energy_consumption.append(cargo_loader_energy_w)
            aircraft_type_list.append(aircraft_type)

            GSE_type.append('cargo loader')
            airport.append(arr_airport)
            air_line.append(airline)
            tail_number.append(tailnumber)
            energy_cons_rate.append(cargo_loader_power_l_w) 
            batt_cap.append(cargo_loader_batt_w)
            start_time.append(cargo_loader_start_time) 
            end_time.append(cargo_loader_end_time_w)
            energy_consumption.append(cargo_loader_energy_w)
            aircraft_type_list.append(aircraft_type)

    GSE_df = pd.DataFrame({'GSE_type': GSE_type, 'airport': airport, 'airline': air_line, 'tail_number':tail_number, 'energy_cons_rate': energy_cons_rate, \
            'batt_cap': batt_cap, 'start_time': start_time, 'end_time': end_time, 'energy_consumption': energy_consumption, 'aircraft_type': aircraft_type_list})
    
    return GSE_df