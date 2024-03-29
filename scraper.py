# -*- coding: utf-8 -*-
import csv
import time
from datetime import date
import datetime
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tkinter import *
from threading import Thread
from tkinter import filedialog
import tkinter.messagebox as tkMessageBox
import shutil

def get_data(filename, last_date):
    with open(filename, encoding='utf-8-sig', newline='') as f:
        reader = csv.reader((line.replace('\0','') for line in f), delimiter=",")
        next(reader)
        for nr,row in enumerate(reader):
            try:
                if datetime.strptime('/'.join(x.zfill(2) for x in row[66].split('/')), '%m/%d/%Y') >= datetime.strptime('/'.join(x.zfill(2) for x in last_date.split('/')), '%m/%d/%Y'):
                    if row[6] == "ISIN":
                        input_data.append({'provider':row[0], "code":row[8], 'name':row[3], 'type':'ISIN'})
                    elif row[6] == "CUSIP":
                        input_data.append({'provider':row[0], "code":row[7], 'name':row[3], 'type':'Cusip'})
                    else:
                        if row[7] != "":
                            input_data.append({'provider':row[0], "code":row[7], 'name':row[3], 'type':'ISIN'})
                        elif row[8] != "":
                            input_data.append({'provider':row[0], "code":row[8], 'name':row[3], 'type':'Cusip'})
            except:
                pass

def UploadAction(event=None):
    global filename
    filename = filedialog.askopenfilename()
    if filename != "":
        csvf.config(text=filename.split("/")[-1])
    else:
        csvf.config(text="Select file")
        filename = None

def Download():

    f = filedialog.asksaveasfile(title='Name a file', initialdir='C:\\', filetypes=(("Comma Delimited", "*.csv*"),), defaultextension='.csv')
    if f is None:
        return

    if f.name[-4:] == ".csv":
        original = r'./Output.csv'
        target = f.name

        shutil.copyfile(original, target)
        tkMessageBox.showinfo(title="Downloaded", message="The Excel file is downloaded.", )

def scraper(input_data_l):
    global input_data
    f = open('Output.csv', 'w', newline='', encoding='UTF-8')
    writer = csv.writer(f)
    writer.writerow(['Fund Provider', 'Fund Name', 'ISIN', 'Cusip', 'Document Type', 'Link', 'Working', 'Document kind'])
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    driver = webdriver.Chrome(options=options)

    for i in input_data_l:
        driver.get("https://www.google.com/")
        driver.get("https://www.google.com/search?q="+(i['provider']+" "+i['code']).replace(" ","+")+"&oq=Aberdeen+Standard+Investments+LU0779217297&sourceid=chrome&ie=UTF-8")
        c_link = driver.current_url
        link = ""
        working = "0"
        document = "link"
        try:
            nr_res = len(WebDriverWait(driver, 10).until(EC.visibility_of_all_elements_located((By.CLASS_NAME, 'g'))))
        except:
            nr_res = 0
        for list_nr in range(0,nr_res):
            document = "link"
            try:
                try:
                    if i['code'] in WebDriverWait(driver, 10).until(EC.visibility_of_all_elements_located((By.CLASS_NAME, 'g')))[list_nr].find_element_by_class_name("TXwUJf").text:
                       continue
                except:
                    pass
                link = WebDriverWait(driver, 10).until(EC.visibility_of_all_elements_located((By.CLASS_NAME, 'g')))[list_nr].find_element_by_tag_name("a").get_attribute("href")
                driver.get(link)
                if "fintel.io" in driver.current_url or "markets.ft.com" in driver.current_url:
                    continue
                if driver.current_url == c_link or driver.current_url[-4:]==".pdf":
                    if PDF.get() == 1:
                        document = "pdf"
                    elif PDF.get() == 0:
                        continue
                working = "1"
                break
            except Exception as e:
                print(e)
                driver.get("https://www.google.com/")
                driver.get("https://www.google.com/search?q="+(i['provider']+" "+i['code']).replace(" ","+")+"&oq=Aberdeen+Standard+Investments+LU0779217297&sourceid=chrome&ie=UTF-8")
                link = ""
                working = "0"
                document = "link"
        print(link)
        if "This site can’t be reached" in driver.page_source:
            print("not working")
            working = "0"
        for type in types:
            if i['type'] == 'ISIN':
                writer.writerow([i['provider'], i['name'], i['code'], "0", document, link, working, type])
            elif i['type'] == 'Cusip':
                writer.writerow([i['provider'], i['name'], "0", i['code'], document, link, working, type])
        f.flush()
        time.sleep(2)
    f.close()

    code.config(state='normal')
    name.config(state='normal')
    csvf.config(state='normal')
    typec.config(state='normal')
    date.config(state='normal')
    justone_btn.config(state='normal')
    add_btn.config(state='normal')
    download.config(state='normal')
    start_btn.config(bg='LightBlue1')
    start_btn.config(text='Start')
    start_btn.config(state='normal')
    input_data = []

def start():
    last_date = DATE.get()
    if last_date != "" and filename != None:
        get_data(filename, last_date)
        code.config(state='disable')
        name.config(state='disable')
        csvf.config(state='disable')
        typec.config(state='disable')
        date.config(state='disable')
        justone_btn.config(state='disable')
        add_btn.config(state='disable')
        start_btn.config(bg="orange")
        start_btn.config(text='Scraping')
        start_btn.config(state='disable')
    else:
        tkMessageBox.showerror("Invalid data", "Please complete at least one pair of data!")
        return
    thread = Thread(target=scraper, args=(input_data,))
    thread.start()

def add():
    code_v = CODE.get()
    fund_name = NAME.get()
    type = TYPEC.get()
    CODE.set("")
    NAME.set("")
    TYPEC.set(code_types[0])
    if code_v != "" and fund_name != "":
        input_data.append({'provider':fund_name, "code":code_v, 'name':"", 'type':type})
    else:
        tkMessageBox.showerror("Invalid data", "Please complete at least one pair of data!")
        return

def j_one():
    code_v = CODE.get()
    fund_name = NAME.get()
    type = TYPEC.get()
    CODE.set("")
    NAME.set("")
    TYPEC.set(code_types[0])
    if code_v != "" and fund_name != "":
        code.config(state='disable')
        name.config(state='disable')
        csvf.config(state='disable')
        typec.config(state='disable')
        date.config(state='disable')
        justone_btn.config(state='disable')
        add_btn.config(state='disable')
        start_btn.config(bg="orange")
        start_btn.config(text='Scraping')
        start_btn.config(state='disable')
        input_data_prov = [{'provider':fund_name, "code":code_v, 'name':"", 'type':type}]
        thread = Thread(target=scraper, args=(input_data_prov,))
        thread.start()
    else:
        tkMessageBox.showerror("Invalid data", "Please complete at least one pair of data!")
        return

if __name__ == '__main__':
    types = [
        "1. Fact Sheet",
        "2. Holdings Report",
        "3. Daily Holdings Report",
        "4. Prospectus",
        "5. Annual Report",
        "6. Semi-Annual Report",
        "7. Statement of Addition Information"
    ]
    input_data = []

    root = Tk()
    root.title("Fund Document Scraper \\ Menu")
    root.geometry("%dx%d+0+0" % (500, 570))
    root.resizable(1, 1)
    root.config(bg="#3399ff")

    title = Frame(root, bd=1, relief=SOLID)
    title.pack(side=TOP, pady=10)
    lbl_display = Label(title, text="Fund Document Scraper", font=('arial', 20))
    lbl_display.pack()
    mainframe = Frame(root, bg='#3399ff')
    mainframe.pack(side=TOP)


    NAME = StringVar()
    CODE = StringVar()
    DATE = StringVar()
    TYPEC = StringVar()
    PDF = IntVar()
    filename = None

    def handle_focus_in(_):
        date.delete(0, END)
        date.config(fg='black')

    def handle_focus_out(_):
        date.delete(0, END)
        date.config(fg='grey')
        date.insert(0, "Example: 12/31/2021")


    lbl_name = Label(mainframe, bg='#3399ff', text="Provider Name:", font=('arial', 12), bd=10)
    lbl_name.grid(row=0, pady=10, sticky=W)
    name = Entry(mainframe, bg='white', textvariable=NAME, font=('arial', 12), width=30)
    name.grid(row=0, pady=10, column=1, sticky=W)

    lbl_code = Label(mainframe, bg='#3399ff', text="CUSIP/ISIN:", font=('arial', 12), bd=10)
    lbl_code.grid(row=1, pady=10, sticky=W)
    code = Entry(mainframe, bg='white', textvariable=CODE, font=('arial', 12), width=30)
    code.grid(row=1, column=1, pady=10, sticky=W)

    lbl_typec = Label(mainframe, bg='#3399ff', text="Fund Code Type:", font=('arial', 12), bd=10)
    lbl_typec.grid(row=2, pady=10, sticky=W)
    code_types = ['ISIN', 'Cusip']
    typec = OptionMenu(mainframe, TYPEC, *code_types)
    typec.grid(row=2, column=1, pady=10, sticky=W)
    TYPEC.set(code_types[0])

    add_btn = Button(mainframe, text='Add', width=20, command=add)
    add_btn.grid(row=3, pady=10, column=0)
    add_btn.config(bg="LightBlue1")

    justone_btn = Button(mainframe, text='Just this', width=20, command=j_one)
    justone_btn.grid(row=3, column=1, columnspan=2, pady=10)
    justone_btn.config(bg="LightBlue1")

    lbl_code = Label(mainframe, bg='#3399ff', text="_______________________________________________", font=('arial', 12), bd=10)
    lbl_code.grid(row=4,columnspan=2)

    lbl_csvf = Label(mainframe, bg='#3399ff', text="Input file:", font=('arial', 12), bd=10)
    lbl_csvf.grid(row=5, pady=10, sticky=W)
    csvf = Button(mainframe, text='Select file', command=UploadAction, width=20)
    csvf.grid(row=5, column=1, sticky=W)

    lbl_date = Label(mainframe, bg='#3399ff', text="Date:", font=('arial', 12), bd=10)
    lbl_date.grid(row=6, pady=10, sticky=W)
    date = Entry(mainframe, bg='white', textvariable=DATE, font=('arial', 12), width=20, fg='grey')
    date.grid(row=6, pady=10, column=1, sticky=W)
    date.insert(0, "Example: 12/31/2021")

    date.bind("<FocusIn>", handle_focus_in)
    date.bind("<FocusOut>", handle_focus_out)

    lbl_pdf = Label(mainframe, bg='#3399ff', text="Scrape PDF documents:", font=('arial', 12), bd=10)
    lbl_pdf.grid(row=7, pady=10, sticky=W)
    pdf = Checkbutton(mainframe, variable=PDF, onvalue=1, offvalue=0)
    pdf.grid(row=7, column=1, pady=10, sticky=W)
    pdf.config(bg="#3399ff")

    download = Button(mainframe, text="Download", width=20, bg="#009ACD", command=Download)
    download.grid(row=8, column=0, pady=10)
    download.config(bg="LightBlue1")
    download.config(state='disable')

    start_btn = Button(mainframe, text='Start', width=20, command=start)
    start_btn.grid(row=8, column=1, columnspan=2, pady=10)
    start_btn.config(bg="LightBlue1")


    root.mainloop()
