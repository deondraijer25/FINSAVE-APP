<?php

require_once(dirname(__FILE__).'/api.php');

class Aqopi2Finsave extends Api {
    
    private function getGuid($headerID) {        
        $sql = "
            select finsave_guid 
              from aqopiapp.t_header h
             where h.header_id = ?
        ";         
        $statement = $this->connection->prepare($sql);        
        $statement->bind_param(
          'i'
        , $headerID
        );        
        if (!$statement->execute()) {
            $statement->close();
            throw new Exception('003 ' . $this->connection->error);
        };                
        $statement->bind_result(
            $result
        );     
        if (!$statement->fetch()) {
            $statement->close();
            throw new Exception('003 Fout. headerID bestaat niet'); 
        }
        $statement->close();
        return $result;
    }
        
    private function saveData($headerID, $tableName) {
        switch ($tableName) {
            case 't_mbd_bankrekening':   
                $proc_name = 'p_process_mbd_bankrekening';
                break;
            case 't_mbd_effectenrekening':
                $proc_name = 'p_process_mbd_effectenrekening';
                break;
            case 't_mbd_hypotheek':
                $proc_name = 'p_process_mbd_hypotheek';
                break;
            case 't_mbd_onroerend_goed':
                $proc_name = 'p_process_mbd_onroerend_goed';
                break;
            case 't_mbd_persoon':
                $proc_name = 'p_process_mbd_persoon';
                break;
            case 't_mbd_premie_lijfrente':
                $proc_name = 'p_process_mbd_premie_lijfrente';
                break;
            case 't_mbd_schuld':
                $proc_name = 'p_process_mbd_schuld';
                break;
            case 't_moh_adres':
                $proc_name = 'p_process_moh_adres';
                break;
            case 't_moh_kind':
                $proc_name = 'p_process_moh_kind';
                break;
            case 't_moh_nationaliteit':
                $proc_name = 'p_process_moh_nationaliteit';
                break;
            case 't_moh_onroerend_goed':
                $proc_name = 'p_process_moh_onroerend_goed';
                break;
            case 't_moh_partner':
                $proc_name = 'p_process_moh_partner';
                break;
            case 't_moh_paspoort':
                $proc_name = 'p_process_moh_paspoort';
                break;
            case 't_moh_persoon':
                $proc_name = 'p_process_moh_persoon';
                break;
            case 't_moh_woz':
                $proc_name = 'p_process_moh_woz';
                break;
            case 't_mpo_ouderdom_pensioen_recht':
                $proc_name = 'p_process_mpo_ouderdom_pensioen_recht';
                break;
            case 't_uwv_algemeen':
                $proc_name = 'p_process_uwv_algemeen';
                break;
            case 't_uwv_werkgever':
                $proc_name = 'p_process_uwv_werkgever';
                break;
            case 't_uwv_werkgever_slip':
                $proc_name = 'p_process_uwv_werkgever_slip';
                break;
            default:
                return "No finsave synchronisation implemented for table $tableName"; 
        }
        
        $query = $this->connection->query("call aqopiapp.$proc_name('$this->guid');"); 
        if (!$query) { 
            throw new Exception($this->connection->error);
        }  
        $result = $query->fetch_object();
        $query->close();
        if (!$result) {
            throw new Exception($this->connection->error);    
        }    
        return $result[0];      
    }
    
    private function getLatestAqopiDatesData() {
        $sql = "
            select latestBatch.owner
                 , latestBatch.type
                 , latestBatch.date
                 , case
                     when datediff(latestBatch.date, setting.refreshed_date) < 0 
                       then 'Y' 
                       else 'N'
                     end as canRefresh
              from (
                select owners.owner 
                     , batch.data_type as type
                     , max(batch.created_date) as date
                  from aqopiapp.t_header batch
                       join (
                         select 0 as is_partner
                              , 'Client' as owner 
                         union
                         select 1 as is_partner
                              , 'Partner' as owner 
                       ) as owners
                         on owners.is_partner = batch.is_partner
                 where batch.finsave_guid = ?
                 group
                    by owners.owner 
                     , batch.data_type
             ) as latestBatch
                  join (
                    select 'MBD' as data_type
                         , date(setting_value) as refreshed_date
                      from aqopiapp.t_setting 
                     where setting_key = 'MBD_REFRESHED'
                    union
                    select 'MOH' as data_type
                         , date(setting_value) as refreshed_date
                      from aqopiapp.t_setting 
                     where setting_key = 'MOH_REFRESHED'
                    union
                    select 'MPO' as data_type
                         , date(setting_value) as refreshed_date
                      from aqopiapp.t_setting 
                     where setting_key = 'MPO_REFRESHED'
                    union
                    select 'UWV' as data_type
                         , date(setting_value) as refreshed_date
                      from aqopiapp.t_setting 
                     where setting_key = 'UWV_REFRESHED'
                  ) as setting
                    on setting.data_type = latestBatch.type
             order
                by latestBatch.owner 
                 , latestBatch.type
        "; 
        $statement = $this->connection->prepare($sql);        
        $statement->bind_param(
          's'
        , $this->guid
        );            
        if (!$statement->execute()) {
            $statement->close();
            throw new Exception('003 ' . $this->connection->error);
        };       
        $statement->bind_result(
            $owner,
            $type,
            $date,
            $canRefresh
        );            
        $result = array();       
        while ($statement->fetch()) {
            $row["owner"] = $owner;
            $row["type"] = $type;
            $row["date"] = $date;
            $row["canRefresh"] = $canRefresh;
            $result[] = $row;
        }                      
        $statement->close();
        return $result;         
    }
    
    private function getLatestAqopiDatesResult() {
        $result = new stdClass();
        try {
            $this->validateGuid(); // may raise exception
            $result->data = $this->getLatestAqopiDatesData();
            $result->message = 'No errors';
            $result->errorcode = '000';
        } catch (Exception $e) {
            $result->data = '';
            $result->errorcode = $this->getExceptionErrorCode($e->getMessage());
            $result->message = $this->getExceptionMessage($e->getMessage());
        } finally {
            return $result;
        }
    }
    
    private function saveResult($headerID, $tableName) {
        $result = new stdClass();
        try {
            $this->guid = $this->getGuid($headerID);
            $this->validateGuid(); // may raise exception
            $result->data = $this->saveData($headerID, $tableName);
            $result->message = 'No errors';
            $result->errorcode = '000';
        } catch (Exception $e) {
            $result->data = '';
            $result->errorcode = $this->getExceptionErrorCode($e->getMessage());
            $result->message = $this->getExceptionMessage($e->getMessage());
        } finally {
            return $result;
        }
    }     
               
    static public function save($connection, $headerID, $tableName)  {
        $aqopi2Finsave = new Aqopi2Finsave($connection, '');
        $result = $aqopi2Finsave->saveResult($headerID, $tableName);
        return $result;         
    }
    
    static public function getLatestAqopiDates($connection, $guid) {
        $aqopi2Finsave = new Aqopi2Finsave($connection, $guid);
        $result = $aqopi2Finsave->getLatestAqopiDatesResult();
        return $result;          
    }
}
