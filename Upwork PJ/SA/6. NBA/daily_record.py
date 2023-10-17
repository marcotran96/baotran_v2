import pygsheets
import pandas as pd
class Gsheet_working:
    def __init__(self, spreadsheet_key, sheet_name, json_file):
        self.spreadsheet_key = spreadsheet_key
        self.sheet_name = sheet_name
        gc = pygsheets.authorize(service_file=json_file)
        sh = gc.open_by_key(spreadsheet_key)
        self.wks = sh.worksheet_by_title(sheet_name)

    def Update_dataframe(self, dataframe, row_num, col_num, clear_sheet=True, copy_head=True):
        wks = self.wks
        sheet_name = self.sheet_name
        if clear_sheet:
            print('clear all data in %s'%sheet_name)
            wks.clear()
        else:
            pass

        total_rows = int(len(dataframe))
        print('Start upload data to %s' % sheet_name)
        if len(dataframe) >= 1:
            dataframe.fillna('', inplace=True)
            wks.set_dataframe(dataframe, (row_num, col_num), copy_head=copy_head)
            print('Upload successful {} lines'.format(len(dataframe)))
        else:
            print('%s not contain value. Check again' % sheet_name)


    def Update_cell(self, cell_name, value):
        wks = self.wks
        wks.update_value(cell_name, value)


    def Read_all_record(self, head = 1, empty_value = ''):
        wks = self.wks
        sheetAll = wks.get_all_records(
            empty_value=empty_value, head=head, majdim='ROWS', numericise_data=True)
        df_insheet = pd.DataFrame.from_dict(sheetAll)
        return df_insheet

    def Append_Current_Sheet(self, dataframe):
        wks = self.wks
        col1 = wks.get_col(1)
        filter_col1 = [i for i in col1 if i is not None and i != '']
        next_empty_row = len(filter_col1) + 1
    
        if dataframe.shape[0] == 0:
            print('Nothing to Insert')
            pass
        else:
            if next_empty_row == 1:
                wks.set_dataframe(dataframe, start=(next_empty_row, 1), copy_head=True)
            else:    
                wks.set_dataframe(dataframe, start=(next_empty_row, 1), copy_head=False)
                print('Insert success {} lines'.format(len(dataframe)))




json_path = r'C:\Users\baotd\Documents\GitHub\Y4A_BA_Team\BotAPI\analytics-data-update-97a3f7a00e57.json'

gc = pygsheets.authorize(service_file=json_path)

spreadsheet_key = '1ffC4WxDobf6dr-AwCRHVjY48qI1nmy1Hy_Rpilyvlig'
sh = gc.open_by_key(spreadsheet_key)

# GET DATA FROM INPUT_SHEET
sheet_name = 'FT'
wks_input = sh.worksheet_by_title(sheet_name)

sheetAll = wks_input.get_all_records(empty_value='', head=1, majdim='ROWS', numericise_data=True)

df = pd.DataFrame.from_dict(sheetAll)