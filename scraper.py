# -*- coding: utf-8 -*-
import csv
import os
import time
from datetime import date
from bs4 import BeautifulSoup
from selenium import webdriver
import requests
import datetime
import random
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from tkinter import *
from threading import Thread
from tkinter import filedialog
import tkinter.messagebox as tkMessageBox

def get_data(filename, last_date):
    input_data = {}
    with open(filename, encoding='utf-8-sig', newline='') as f:
        reader = csv.reader((line.replace('\0','') for line in f), delimiter=",")
        next(reader)
        for nr,row in enumerate(reader):
            try:
                if datetime.strptime('/'.join(x.zfill(2) for x in row[66].split('/')), '%m/%d/%Y') >= datetime.strptime('/'.join(x.zfill(2) for x in last_date.split('/')), '%m/%d/%Y'):
                    if row[6] == "ISIN":
                        input_data[nr] = {'provider':row[0], "code":row[8], 'name':row[3]}
                    elif row[6] == "CUSIP":
                        input_data[nr] = {'provider':row[0], "code":row[7], 'name':row[3]}
                    else:
                        if row[7] != "":
                            input_data[nr] = {'provider':row[0], "code":row[7], 'name':row[3]}
                        elif row[8] != "":
                            input_data[nr] = {'provider':row[0], "code":row[8], 'name':row[3]}
            except:
                pass
    return input_data


def UploadAction(event=None):
    global filename
    filename = filedialog.askopenfilename()
    if filename != "":
        csvf.config(text="File Selected")


def scraper(input_data):
    f = open('Output.csv', 'w', newline='', encoding='UTF-8')
    writer = csv.writer(f)
    writer.writerow(['Fund Provider', 'Fund Name ', 'Fund Code Cusip/ISIN', 'Document Type', 'Link', 'Working'])
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    driver = webdriver.Chrome(options=options)

    for i in input_data:
        driver.get("https://www.google.com/")
        driver.get("https://www.google.com/search?q="+(input_data[i]['provider']+" "+input_data[i]['code']).replace(" ","+")+"&oq=Aberdeen+Standard+Investments+LU0779217297&sourceid=chrome&ie=UTF-8")
        c_link = driver.current_url
        link = ""
        doc = ""
        working = "0"
        try:
            nr_res = len(WebDriverWait(driver, 10).until(EC.visibility_of_all_elements_located((By.CLASS_NAME, 'g'))))
        except:
            nr_res = 0
        for list_nr in range(0,nr_res):
            try:
                try:
                    if input_data[i]['code'] in WebDriverWait(driver, 10).until(EC.visibility_of_all_elements_located((By.CLASS_NAME, 'g')))[list_nr].find_element_by_class_name("TXwUJf").text:
                       continue
                except:
                    pass
                link = WebDriverWait(driver, 10).until(EC.visibility_of_all_elements_located((By.CLASS_NAME, 'g')))[list_nr].find_element_by_tag_name("a").get_attribute("href")
                driver.get(link)
                if driver.current_url == c_link or driver.current_url[-4:]==".pdf":
                    doc = 'pdf'
                else:
                    doc = 'link'
                working = "1"
                break
            except Exception as e:
                print(e)
                driver.get("https://www.google.com/")
                driver.get("https://www.google.com/search?q="+(input_data[i]['provider']+" "+input_data[i]['code']).replace(" ","+")+"&oq=Aberdeen+Standard+Investments+LU0779217297&sourceid=chrome&ie=UTF-8")
                link = ""
                doc = ""
                working = "0"
        print(link)
        print(doc)
        if "This site can’t be reached" in driver.page_source:
            print("not working")
            working = "0"
        writer.writerow([input_data[i]['provider'], input_data[i]['name'], input_data[i]['code'], doc, link, working])
        f.flush()
        time.sleep(2)
    f.close()



def start():
    code = CODE.get()
    fund_name = name.get()
    last_date = DATE.get()
    print(code)
    print(fund_name)
    print(last_date)
    if code != "" and fund_name != "":
        input_data = {}
        input_data[0] = {'provider':fund_name, "code":code, 'name':""}
    elif last_date != "" and filename != None:
        print("here")
        input_data = get_data(filename, last_date)
    else:
        tkMessageBox.showerror("Invalid data", "Please complete at least one pair of data!")
        return
    print(input_data)
    thread = Thread(target=scraper, args=(input_data,))
    thread.start()


if __name__ == '__main__':

    root = Tk()
    root.title("Automation Software \\ Menu")
    root.geometry("%dx%d+0+0" % (500, 450))
    root.resizable(1, 1)
    root.config(bg="#3399ff")

    title = Frame(root, bd=1, relief=SOLID)
    title.pack(side=TOP, pady=10)
    lbl_display = Label(title, text="Automation Software", font=('arial', 20))
    lbl_display.pack()
    mainframe = Frame(root, bg='#3399ff')
    mainframe.pack(side=TOP)


    NAME = StringVar()
    CODE = StringVar()
    DATE = StringVar()
    filename = None

    def handle_focus_in(_):
        date.delete(0, END)
        date.config(fg='black')

    def handle_focus_out(_):
        date.delete(0, END)
        date.config(fg='grey')
        date.insert(0, "Example: 12/31/2021")

    def handle_enter(txt):
        print(date.get())
        handle_focus_out('dummy')

    lbl_name = Label(mainframe, bg='#3399ff', text="Provider Name:", font=('arial', 12), bd=10)
    lbl_name.grid(row=0, pady=10, sticky=W)
    name = Entry(mainframe, bg='white', textvariable=NAME, font=('arial', 12), width=32, fg='grey')
    name.grid(row=0, pady=10, column=1, sticky=W)

    lbl_code = Label(mainframe, bg='#3399ff', text="CUSIP/ISIN:", font=('arial', 12), bd=10)
    lbl_code.grid(row=1, pady=10, sticky=W)
    code = Entry(mainframe, bg='white', textvariable=CODE, font=('arial', 12), width=32)
    code.grid(row=1, column=1, pady=10, sticky=W)

    lbl_code = Label(mainframe, bg='#3399ff', text="_______________________________________________", font=('arial', 12), bd=10)
    lbl_code.grid(row=2,columnspan=2)

    lbl_csvf = Label(mainframe, bg='#3399ff', text="Input file:", font=('arial', 12), bd=10)
    lbl_csvf.grid(row=3, pady=10, sticky=W)
    csvf = Button(mainframe, text='Select a file', command=UploadAction, width=25)
    csvf.grid(row=3, column=1, sticky=W)

    lbl_date = Label(mainframe, bg='#3399ff', text="Date:", font=('arial', 12), bd=10)
    lbl_date.grid(row=4, pady=10, sticky=W)
    date = Entry(mainframe, bg='white', textvariable=DATE, font=('arial', 12), width=32, fg='grey')
    date.grid(row=4, pady=10, column=1, sticky=W)
    date.insert(0, "Example: 12/31/2021")

    date.bind("<FocusIn>", handle_focus_in)
    date.bind("<FocusOut>", handle_focus_out)
    date.bind("<Return>", handle_enter)


    start_btn = Button(mainframe, text='Start', width=25, command=start)
    start_btn.grid(row=5, column=1, columnspan=2, pady=10, sticky=W)
    start_btn.config(bg="LightBlue1")


    root.mainloop()