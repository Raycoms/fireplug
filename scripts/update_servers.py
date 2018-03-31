import sys
import re
import subprocess

if len(sys.argv) < 6:
	print(sys.argv[0] +" cluster_size total_clusters latency partial_ip servers...")
	print("ex.: update_servers.py 4 2 1 172.16.52 2 3 4 5 6 7 8 9")
	sys.exit(-1)

cluster_size = int(sys.argv[1])
total_clusters = int(sys.argv[2])
latency = int(sys.argv[3])
partial_ip = sys.argv[4]
servers = sys.argv[5:]

total_servers = len(servers)
initial_global_port = 11300
initial_local_port = 11500
port_step = 10 if total_servers < 10 else 5

for i in range(0, total_servers):
	servers[i] = int(servers[i])

if len(servers) != cluster_size *total_clusters:
    (print "servers are not cluster size times total clusters")
	sys.exit(-1)

print("cluster_size: %d"%cluster_size)
print("total_clusters: %d"%total_clusters)
print("latency: %d"%latency)
print("partial_ip: " +partial_ip)
print("servers:")
print(servers)

# create global strings
global_strings = [] #a list[server]
print('    global:')
for i in range(0, total_servers):
	ip = "%s.%d"%(partial_ip, servers[i])
	port = initial_global_port + (port_step *i)
	global_strings.append("%d %s %d"%(i, ip, port))
	print(global_strings[i])

# create local strings
local_strings = [] #a matrix [cluster][server]
for cluster in range(0, total_clusters):
	print('    local: %d'%(cluster))
	local_strings.append([])
	for i in range(0, cluster_size):
		server = servers[cluster +(i*total_clusters)]
		ip = "%s.%d"%(partial_ip, server)
		port = initial_local_port + (100 *cluster) + (port_step *i)
		local_strings[cluster].append("%d %s %d"%(i, ip, port))
		print(local_strings[cluster][i])

# update lines from files by replacing strings
def update_lines(lines, strings):
	strings_count = 0
	for i in range(0, len(lines)):
		match = re.match('[0-9]+ ', lines[i])
		if match:
			number = int(match.group())
			if number < 7000:
				if number < len(strings):
					lines[i] = strings[number] +"\n"
					strings_count = number
				else:
					lines[i] = "#"+lines[i]
			else:
				lastline = lines[i]
				lines = lines[0:i]
				for string in range(strings_count+1, len(strings)):
					lines.append(strings[string] +'\n')
				lines.append(lastline)
                                return lines
	return lines

# write global file
gfile = open('thesis/global/config/hosts.config', 'r')
lines = gfile.readlines()
gfile.close()

lines = update_lines(lines, global_strings)

gfile = open('thesis/global/config/hosts.config', 'w')
gfile.writelines(lines)
gfile.close()

# write local files
for i in range(0, total_clusters):
	lfile = open('thesis/local%d/config/hosts.config'%i, 'r')
	lines = lfile.readlines()
	lfile.close()

	lines = update_lines(lines, local_strings[i])

	lfile = open('thesis/local%d/config/hosts.config'%i, 'w')
	lfile.writelines(lines)
	lfile.close()


# latency

#updates latency file lines
def update_latency_lines(lines, string):
	for i in range(0, len(lines)):
		match = re.match('HOSTS=', lines[i])
		if match:
			lines[i] = 'HOSTS=' + string + '\n'
	return lines

# stop latencies
for server in range(0, total_servers):
	cluster = server % total_clusters
	host = 'nova-%d'%server
	command = 'addlatency%d.sh stop'%cluster
	subprocess.Popen(['ssh', host, command], shell=False)


# write latency files
if latency == 1:
	# set latency strings
	print("Latency hosts: ")
	latency_strings = [] #a list of strings
	for cluster in range(0, total_clusters):
		latency_strings.append('"')
		for server in range(0, total_servers):
			if server % total_clusters != cluster:
				latency_strings[cluster] += '%s.%d '%(partial_ip, servers[server])
		latency_strings[cluster] = latency_strings[cluster][:-1] + '"'
		print("HOSTS for local%d: "%cluster + latency_strings[cluster])


	for i in range(0, total_clusters):
		lfile = open('addlatency%d.sh'%i, 'r')
		lines = lfile.readlines()
		lfile.close()

		lines = update_latency_lines(lines, latency_strings[i])

		lfile = open('addlatency%d.sh'%i, 'w')
		lfile.writelines(lines)
		lfile.close()

	# restart latencies
	for server in range(0, total_servers):
		cluster = server % total_clusters
		host = 'nova-%d'%server
		command = 'addlatency%d.sh start'%cluster
		subprocess.Popen(['ssh', host, command], shell=False)


# BAG_HOSTS
print('')
export_servers = 'export BAG_HOSTS="'
for server in servers:
	export_servers += "%s.%d "%(partial_ip, server)
export_servers = export_servers[:-1]
export_servers += '"'
print(export_servers)