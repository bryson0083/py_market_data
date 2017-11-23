import os

def MAIN_MV_FILE():
	print("搬移前一天選股結果檔案...")
	cwd = os.getcwd()
	file_ls = []
	for file in os.listdir(cwd):
		if file.startswith("STOCK_") and file.endswith(".xlsx"):
			#print(file)
			file_ls.append(file)

	#print(file_ls)

	for mv_file in file_ls:
		print(mv_file)
		tar_file = cwd + "\\" + mv_file
		des_file = cwd + "\\stock_select_history\\" + mv_file

		try:
			os.rename(tar_file, des_file)
			print(mv_file + "移動檔案完畢.")
		except Exception as e:
			print(mv_file + "移動檔案失敗.")
			print(e.args)

	print("搬移檔案結束...")

if __name__ == "__main__":
	MAIN_MV_FILE()	