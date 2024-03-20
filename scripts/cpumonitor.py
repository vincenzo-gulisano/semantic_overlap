import psutil
import csv
import time
import argparse

def monitor_cpu(pid, output_csv):
    with open(output_csv, 'w', newline='') as csv_file:
        fieldnames = ['Time (seconds)', 'CPU Usage (%)']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
       
        process = psutil.Process(pid)
       
        try:
            while True:
                cpu_percent = process.cpu_percent(interval=1.0)
                elapsed_time = time.time()

                writer.writerow({'Time (seconds)': int(elapsed_time), 'CPU Usage (%)': cpu_percent})
                csv_file.flush()
        except KeyboardInterrupt:
            pass
        except psutil.NoSuchProcess:
            print(f"Process with PID {pid} not found. Exiting.")
        except Exception as e:
            print(f"An error occurred: {e}")

def main():
    parser = argparse.ArgumentParser(description="Monitor CPU consumption of a process and write to a CSV file.")
    parser.add_argument("--pid", type=int, required=True, help="PID of the process to monitor")
    parser.add_argument("--output", required=True, help="Output CSV file")
    args = parser.parse_args()

    monitor_cpu(args.pid, args.output)

if __name__ == '__main__':
    main()
