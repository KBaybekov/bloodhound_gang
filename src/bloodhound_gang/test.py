from pathlib import Path

f = Path('/run/user/59206388/gvfs/sftp:host=vu10-2-027/home/PAK-CSPMZ/kbajbekov/mnt/mnt/cephfs8_rw/nanopore2/service/code/github/bloodhound_gang/src/bloodhound_gang/useful_scripts/772015791401-basic-dna-r1041_used_model.txt')

print(f.read_text().split()[0].strip())