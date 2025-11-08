<?php

class Api {

    const API_NAME = 'aqopiapp-api';
    
    protected $connection;
    protected $guid;
    protected $useTranslations;
    
    private function logApiError($apiName, $guid, $errorMessage) {
        $sql = "
            insert
              into finsave.t_api_errorlog (
                   api_name
                 , finsave_guid
                 , error_message
                 ) 
            select ?
                 , ? 
                 , ?
        "; 
        $statement = $this->connection->prepare($sql);          
        if (!$statement) {
            $error = $this->connection->error . "(processing $errorMessage)";
            return "SQL Exception in Api->logApiError: $error";  
        }
        $statement->bind_param('sss', $apiName, $guid, $errorMessage);        
        if (!$statement->execute()) { 
            return "Exception in Api->logApiError: $statement->error"; 
        }
        $statement->close(); 		
        return '';
    }    
    
    protected function __construct($connection, $guid) {
        $this->connection = $connection;
        $this->guid = $guid;
        $this->useTranslations = true;
    }   
    
    protected function validateGuid() {
        $sql = "
            select 1
              from hypdatabase.t_hypotheek 
             where hypotheekId = ?
        ";              
        $statement = $this->connection->prepare($sql); 
        if (!$statement) {
            throw new Exception('SQL Exception in Api->validateGuid: ' . $this->connection->error);
        }
        $statement->bind_param(
          's'
        , $this->guid
        );  
        if (!$statement->execute()) {
            throw new Exception('Exception in Api->validateGuid: ' . $this->connection->error);
        }; 
        $statement->bind_result(
            $result
        );
        if (!$statement->fetch()) {
            throw new Exception("Exception in Api->validateGuid: guid $this->guid does not exist");    
        }
        $statement->close();  
    }
    
    protected function getLastInsertID() {
        $sql = "
            select last_insert_id()
        ";              
        $statement = $this->connection->prepare($sql);  
        if (!$statement->execute()) {
            throw new Exception('003 ' . $this->connection->error);
        }; 
        $statement->bind_result(
            $result
        );
        if (!$statement->fetch()) {
            $result = 0;    
        }
        $statement->close();  
        return $result;
    }       
    
    protected function getHypdatabaseObjectID() {
        $sql = "
            select finsave.f_get_hypdatabase_object_id()
        ";              
        $statement = $this->connection->prepare($sql);  
        if (!$statement) {
            throw new Exception('003 ' . $this->connection->error);            
        }
        if (!$statement->execute()) {
            throw new Exception('003 ' . $this->connection->error);
        }; 
        $statement->bind_result(
            $result
        );
        if (!$statement->fetch()) {
            throw new Exception('003 Error in Api.getHypdatabaseObjectID. No result from select finsave.f_get_hypdatabase_object_id()');    
        }
        $statement->close();  
        return $result;
    }       
    
    protected function getExceptionMessage($exceptionMsg) {
        $errorCode = $this->getExceptionErrorCode($exceptionMsg);
        if (substr($exceptionMsg, 0, 3) == $errorCode) {
            $result = substr($exceptionMsg, 3);    
        } else {
            $result = $exceptionMsg;
        }
        if ($errorCode == '003') {
            $logResult = $this->logApiError(self::API_NAME, $this->guid, $exceptionMsg);
            if ($logResult) {
                $result = "$result; $logResult"; // show exception from logApiError too
            }
        }
        return $result;
    }
    
    protected function getExceptionErrorCode($exceptionMsg) {
        /*
          001: client must logoff
          002: client can show error
          003: other error
        */
        $result = substr($exceptionMsg, 0, 3);
        if ($result != '001' && $result != '002') {
            $result = '003';  
        }
        return $result;
    }  
    
    protected function getUserIdByFirebaseUid($firebase_uid) {  
        // Maybe implement some caching here
        $sql = "
            select user_id 
              from userapp.t_user
             where firebase_uid = ?
        ";         
        $statement = $this->connection->prepare($sql);  
        if (!$statement) {
            throw new Exception('003 ' . $this->connection->error);   
        }
        $statement->bind_param(
          's'
        , $firebase_uid
        );        
        if (!$statement->execute()) {
            $statement->close();
            throw new Exception('003 ' . $this->connection->error);
        };     
        $statement->store_result();           
        $statement->bind_result(
            $result
        );     
        if (!$statement->fetch()) {
            $statement->close();
            throw new Exception("002 Error. No user found for firebase_uid $firebase_uid"); 
        }
        $statement->free_result();
        $statement->close();
        return $result;
    }  
    
    protected function logJson($batchId, $data) {
        $data = json_encode($data);
        $sql = "
            insert
              into aqopiapp.t_log
                 ( header_rid
                 , json
                 )
            select ?
                 , ?
        ";         
        $statement = $this->connection->prepare($sql);  
        if (!$statement) {
            throw new Exception('003 ' . $this->connection->error);   
        }
        $statement->bind_param(
          'is'
        , $batchId
        , $data
        );        
        if (!$statement->execute()) {
            $statement->close();
            throw new Exception('003 ' . $this->connection->error);
        }; 
        $statement->close();    
    }    
}   