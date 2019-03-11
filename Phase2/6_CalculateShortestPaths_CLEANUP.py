import os
import subprocess

p = subprocess.Popen([r"C:\Program Files (x86)\pgAdmin III\1.22\pg_dump.exe", "--host", "localhost"], stdout = subprocess.PIPE)


["C:\Program Files (x86)\pgAdmin III\1.22\pg_dump.exe", "--host", "localhost"]
--port 5432 
--username "postgres" 
--no-password  
--format custom 
--verbose 
--file "C:\Users\model-ws\Desktop\temp.backup" 
--table "public.\"TSYS\"" "postgres"

###dump to modeling or external drive (faster)
###use TAR compression

