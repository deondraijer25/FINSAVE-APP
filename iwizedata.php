<?php

require_once(dirname(__FILE__).'/api.php');

class IwizeData extends Api {
    const SUPPORTED_COLUMN_TYPES = ["boolean"=>"i","integer"=>"i","double"=>"d","string"=>"s"];
    
    private $batchId;
    private $tableColumnList;
    
    private function __construct($connection, $batchId) {
        parent::__construct($connection, '');
        $this->batchId = $batchId;
    } 
    
    private function isSupported($columnValue) {
        return array_key_exists(gettype($columnValue), self::SUPPORTED_COLUMN_TYPES);  
    }
    
    private function getParameterType($columnValue) {
        if (!$this->isSupported($columnValue)) {
            throw new Exception('Exception in '. __CLASS__ . '->' . __FUNCTION__ . ": $columnValue is not suppported");  
        }
        return self::SUPPORTED_COLUMN_TYPES[gettype($columnValue)];
    }
    
    private function refreshTableColumnList($tableName) {
        $this->tableColumnList = array();
        $sql = "
            select column_name 
              from information_schema.columns 
             where table_name like concat('', ?, '')
               and table_schema like 'iwize'
        ";           
        $statement = $this->connection->prepare($sql);   
        if (!$statement) {
            throw new Exception('003 ' . $this->connection->error);     
        }
        $statement->bind_param(
          's'
        , $tableName
        );    
        if  (!$statement) {
            throw new Exception('Exception in '. __CLASS__ . '->' . __FUNCTION__ . ': ' . $this->connection->error);    
        }
        if (!$statement->execute()) {
            throw new Exception('Exception in '. __CLASS__ . '->' . __FUNCTION__ . ': ' . $statement->error);
        }
        $statement->bind_result(
            $columnName
        );        
        while ($statement->fetch()) {
            $this->tableColumnList[] = $columnName;
        }         
        $statement->close();
    }
    
    private function isAvailable($tableName, $columnName) {                 
        return in_array($columnName, $this->tableColumnList);  
    } 
    
    private function addRecordItemData($tableName, $record, $parentColumn = null, $parentValue = null) {
        $result = 0;
        $array_params = array();
        $sql_insert = "
            insert
              into iwize.$tableName
        ";
        $sql_columns = "
                 ( batch_rid
        ";
        $sql_parameters = "
                 )
            select ?
        ";
        $parameterTypes = "i";    
        $array_params[] = &$parameterTypes; // pass by reference
        $array_params[] = &$this->batchId; // add reference to first parameterValue
        if (is_array($record)) {
            foreach ($record as $columnName => $columnValue) {
                if (    (!is_null($columnValue)) 
                     && ($this->isSupported($columnValue))
                     && ($this->isAvailable($tableName, $columnName))
                   ) {                
                    $sql_columns .= "
                         , $columnName
                    ";
                    $sql_parameters .= "
                         , ?
                    ";
                    if (gettype($columnValue) == 'string') {
                        $columnValue = htmlspecialchars_decode($columnValue);
                        // strings may contain \u00e9 which must be converted to é
                        $columnValue = preg_replace('/\\\\u([\da-fA-F]{4})/', '&#x\1;', $columnValue); 
                        $columnValue = html_entity_decode($columnValue);
                        $record[$columnName] = $columnValue;
                    }
                    $parameterTypes .= $this->getParameterType($columnValue);
                    $array_params[] = &$record[$columnName]; // add reference to $columnValue
                }
            }
            if (($parentColumn) && ($parentValue)) {
                if (    ($this->isSupported($parentValue))
                     && ($this->isAvailable($tableName, $parentColumn))
                   ) {
                    $sql_columns .= "
                         , $parentColumn
                    ";
                    $sql_parameters .= "
                         , ?
                    ";
                    $parameterTypes .= $this->getParameterType($parentValue);
                    $array_params[] = &$parentValue; 
                } else {
                    throw new Exception('Exception in '. __CLASS__ . '->' . __FUNCTION__ . ": unable to insert childrecord in $tableName with value $parentValue in parentcolumn $parentColumn");    
                }
            }        
            $sql = $sql_insert . $sql_columns . $sql_parameters;      
            $statement = $this->connection->prepare($sql);  
            if (!$statement) {
                throw new Exception('Exception in '. __CLASS__ . '->' . __FUNCTION__ . ": $sql is not correct sql");
            }
            if (!call_user_func_array(array($statement, 'bind_param'), $array_params)) {
                throw new Exception('Exception in '. __CLASS__ . '->' . __FUNCTION__ . ' with params: ' . implode(" ", $array_params));
            }
            if (!$statement->execute()) {    
                throw new Exception('Exception in '. __CLASS__ . '->' . __FUNCTION__ . ': ' . $this->connection->error . ' sql:' . $sql . ' params:' . implode(" ", $array_params));
            };
            $statement->close(); 
            $result = $this->getLastInsertID();
        }
        return $result;
    }
        
    private function addRecordListData($tableName, $data) {
        $result = array();
        if (is_array($data)) {
            foreach ($data as $record) {
                $result[] = $this->addRecordItemData($tableName, $record);
            }
        }
        return $result;
    }     

    private function newBatchData($guid, $firebase_uid, $firebase_email, $uuid, $isPartner) {
        $sql = "
            insert
              into iwize.t_batch
                 ( finsave_guid
                 , userapp_user_rid
                 , firebase_uid
                 , firebase_username
                 , iwize_uuid
                 , is_partner
                 )
            select ?
                 , ?
                 , ?
                 , ?
                 , ?
                 , ?
        ";  
        $userappUserId = $this->getUserIdByFirebaseUid($firebase_uid);       
        $statement = $this->connection->prepare($sql);     
        if  (!$statement) {
            throw new Exception('SQL Exception in '. __CLASS__ . '->' . __FUNCTION__ . ': ' . $this->connection->error);    
        }     
        $statement->bind_param(
          'sisssi'
        , $guid
        , $userappUserId
        , $firebase_uid
        , $firebase_email
        , $uuid
        , $isPartner
        );        
        if (!$statement->execute()) {  
            throw new Exception('Exception in '. __CLASS__ . '->' . __FUNCTION__ . ': ' . $statement->error);
        }; 
        $statement->close(); 
        $result = $this->getLastInsertID();
        return $result;
    }
        
    private function addChildRecordListData($tableName, $data, $parentColumn, $parentValue) {
        $result = array();
        foreach ($data as $record) {
            $result[] = $this->addRecordItemData($tableName, $record, $parentColumn, $parentValue);
        }
        return $result;
    }    
   
    private function addChildRecordListResult($tableName, $data, $parentColumn, $parentValue) {
        $result = new stdClass();
        $result->data = new stdClass();
        try {                        
            $this->refreshTableColumnList($tableName);
            $result->data->iwize = $this->addChildRecordListData($tableName, $data, $parentColumn, $parentValue);
            $result->errorcode = '000';
            $result->message = 'No errors';
        } catch (Exception $e) {
            $result->data = '';
            $result->errorcode = $this->getExceptionErrorCode($e->getMessage());
            $result->message = $this->getExceptionMessage($e->getMessage());
        } finally {
            return $result;
        }
    }
    
    private function addChildRecordItemResult($tableName, $data, $parentColumn, $parentValue) {
        $result = new stdClass();
        $result->data = new stdClass();
        try {                        
            $this->refreshTableColumnList($tableName);
            $result->data->iwize = $this->addRecordItemData($tableName, $data, $parentColumn, $parentValue);
            $result->errorcode = '000';
            $result->message = 'No errors';
        } catch (Exception $e) {
            $result->data = '';
            $result->errorcode = $this->getExceptionErrorCode($e->getMessage());
            $result->message = $this->getExceptionMessage($e->getMessage());
        } finally {
            return $result;
        }
    }

    private function addRecordListResult($tableName, $data) {
        $result = new stdClass();
        $result->data = new stdClass();
        try {   
            $this->refreshTableColumnList($tableName);  
            $result->data->iwize = $this->addRecordListData($tableName, $data);
            $result->errorcode = '000';
            $result->message = 'No errors';
        } catch (Exception $e) {
            $result->errorcode = $this->getExceptionErrorCode($e->getMessage());
            $result->message = $this->getExceptionMessage($e->getMessage());
        } finally {
            return $result;
        }
    } 
    
    private function addRecordItemResult($tableName, $data) {
        $result = new stdClass();
        $result->data = new stdClass();
        try {                        
            $this->refreshTableColumnList($tableName);
            $result->data->iwize = $this->addRecordItemData($tableName, $data);
            $result->errorcode = '000';
            $result->message = 'No errors';
        } catch (Exception $e) {
            $result->errorcode = $this->getExceptionErrorCode($e->getMessage());
            $result->message = $this->getExceptionMessage($e->getMessage());
        } finally {
            return $result;
        }
    }      
    
    private function newBatchResult($guid, $firebase_uid, $firebase_email, $iwizeUuid, $isPartner) {
        $result = new stdClass();
        try {                     
            $result->data = $this->newBatchData($guid, $firebase_uid, $firebase_email, $iwizeUuid, $isPartner);
            $result->errorcode = '000';
        } catch (Exception $e) {
            $result->errorcode = $this->getExceptionErrorCode($e->getMessage());
            $result->message = $this->getExceptionMessage($e->getMessage());
        } finally {
            return $result;
        }
    }      
        
    static public function addRecordList($connection, $batchId, $tableName, $data) {
        $iwizeData = new IwizeData($connection, $batchId);
        $result = $iwizeData->addRecordListResult($tableName, $data);
        return $result;         
    }
   
    static public function addRecordItem($connection, $batchId, $tableName, $data) {
        $iwizeData = new IwizeData($connection, $batchId);
        $result = $iwizeData->addRecordItemResult($tableName, $data);
        return $result;         
    }
 
    static public function addChildRecordList($connection, $batchId, $tableName, $data, $parentColumn, $parentValue)  {
        $iwizeData = new IwizeData($connection, $batchId);
        $result = $iwizeData->addChildRecordListResult($tableName, $data, $parentColumn, $parentValue);
        return $result;         
    }
    
    static public function addChildRecordItem($connection, $batchId, $tableName, $data, $parentColumn, $parentValue)  {
        $iwizeData = new IwizeData($connection, $batchId);
        $result = $iwizeData->addChildRecordItemResult($tableName, $data, $parentColumn, $parentValue);
        return $result;         
    }    

    static public function newBatch($connection, $guid, $firebase_uid, $firebase_email, $iwizeUuid, $isPartner) {
        $iwizeData = new IwizeData($connection, 0);
        $result = $iwizeData->newBatchResult($guid, $firebase_uid, $firebase_email, $iwizeUuid, $isPartner);
        return $result;
    }
}
