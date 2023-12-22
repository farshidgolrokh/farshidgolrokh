DELIMITER //
START TRANSACTION;
DROP PROCEDURE IF EXISTS nodedb.vouchersale;
CREATE DEFINER=root@localhost PROCEDURE nodedb.vouchersale(
  IN Vorderid INT,
  IN Vinputdiscount DOUBLE,
  IN Vtotalor DOUBLE,
  IN Vcreatby VARCHAR(45)  CHARSET utf8mb4 COLLATE utf8mb4_persian_ci,
  IN Vmonth INT,
  IN VNumberSale VARCHAR(45)  CHARSET utf8mb4 COLLATE utf8mb4_persian_ci
)
NOT DETERMINISTIC CONTAINS SQL SQL SECURITY DEFINER
BEGIN
  DECLARE VDLIDt,VDLIDdis,VDLIDDcommission,VSaleOrderID,VDLIDCcommission,VDLIDD,Vcashamound,Vvoucherid,Vfiscalyear,Vbusinessunitid,Vfund,VOPid,VDLIDC INT DEFAULT 0 ;
  DECLARE Veventdate DATETIME ;
  DECLARE Vformatted VARCHAR(20);
  DECLARE VSLIDs1,VSLIDs0,VSLIDcost1,VSLIDcost0,VSLIDcommission1,VSLIDcommission0,VSLIDsd1,VSLIDsd0,VSLIDsca1,VSLIDsca0,VSLIDst1,VSLIDst0 SMALLINT DEFAULT 0 ;
  DECLARE VVoucherTypeID INT DEFAULT 4;
  DECLARE VVoucherTypeIDcost INT DEFAULT 5;
  DECLARE VVTypeIDcommission INT DEFAULT 9;
  DECLARE VQty,Vunitprice,Vdiscount,Vtax,Vcost,VAmount,VAmountLine,Vtotaltax,VSUMAmount,VQtyinv,Vcommission,VStandardCost DOUBLE DEFAULT 0.0 ;
  DECLARE VdescriptionItem,Vdes,VdesComm,VProductname,VProductcode,Vunitname,VdesVoucher,VdesVoucherItem,VdesVoucherItemcomm,Vdestr,Vnameofcustomer VARCHAR(255)  CHARSET utf8mb4 COLLATE utf8mb4_persian_ci ;
  DECLARE done INT DEFAULT FALSE;
  
  DECLARE cursorSALEITEM CURSOR FOR
  SELECT producttransaction.id,
                      producttransaction.ProductID,
                      producttransaction.Qty,
                      producttransaction.unitprice,
                      producttransaction.discount,
                      producttransaction.tax,
                      producttransaction.commission,
                      products.code,
                      products.name as productname,
                      products.StandardCost,
                      units.name as unitname
                from golrokh.producttransaction as producttransaction
                Inner Join golrokh.vw_product as products on products.DLID=producttransaction.ProductID
                Inner Join golrokh.units as units on units.id=products.unit
                        Where producttransaction.orderid = Vorderid;
  DECLARE CONTINUE HANDLER
        FOR NOT FOUND SET done = TRUE;

        CREATE TEMPORARY TABLE nodedb.temp_saleorders AS
            SELECT * from golrokh1402.vw_saleorders as saleorders Where saleorders.orderid = Vorderid;
        
        SELECT id INTO VSaleOrderID FROM nodedb.temp_saleorders;
        SELECT SaleEmployeeDLID INTO VDLIDCcommission FROM nodedb.temp_saleorders;
        SELECT DLID INTO VDLIDD FROM nodedb.temp_saleorders;
        SELECT DLname INTO Vnameofcustomer FROM nodedb.temp_saleorders;
        SELECT cashamound INTO Vcashamound FROM nodedb.temp_saleorders;
        SELECT fund INTO Vfund FROM nodedb.temp_saleorders;
        SELECT orderdate INTO Veventdate FROM nodedb.temp_saleorders;
        SELECT BusinessUnitID INTO Vbusinessunitid FROM nodedb.temp_saleorders;
        SELECT fiscalyear INTO Vfiscalyear FROM nodedb.temp_saleorders;
        DROP TEMPORARY TABLE nodedb.temp_saleorders;
        
        CREATE TEMPORARY TABLE nodedb.temp_defaultdls AS
            SELECT DLID FROM golrokh.defaultdls;
        
        SELECT DLID INTO VDLIDt FROM nodedb.temp_defaultdls WHERE id = 1;
        SELECT DLID INTO VDLIDdis FROM nodedb.temp_defaultdls WHERE id = 2;
        SELECT DLID INTO VDLIDDcommission FROM nodedb.temp_defaultdls WHERE id = 3;
        
        DROP TEMPORARY TABLE nodedb.temp_defaultdls;
        
        SET VdesVoucher=CONCAT('بابت فاکتور فروش شماره ',VNumberSale);
        SET VdesVoucherItem=CONCAT('بابت فاکتور فروش شماره  : ',VNumberSale,' - ( {1} {2} {3} به قرار هر {2} :');
        SET VdesVoucherItemcomm=CONCAT('بابت کمیسیون فاکتور فروش شماره  : ',VNumberSale,' - ( {1} % {2} ) ');
        
        CREATE TEMPORARY TABLE nodedb.temp_voucherpattern AS
          select  VoucherTypeID, IsDebit, slid from golrokh.voucherpattern;
        SELECT slid INTO VSLIDs1 FROM nodedb.temp_voucherpattern WHERE VoucherTypeID = 4 AND IsDebit=1 ;
        SELECT slid INTO VSLIDs0 FROM nodedb.temp_voucherpattern WHERE VoucherTypeID = 4 AND IsDebit=0 ;
        SELECT slid INTO VSLIDcost1 FROM nodedb.temp_voucherpattern WHERE VoucherTypeID = 5 AND IsDebit=1 ;
        SELECT slid INTO VSLIDcost0 FROM nodedb.temp_voucherpattern WHERE VoucherTypeID = 5 AND IsDebit=0 ;
        SELECT slid INTO VSLIDcommission1 FROM nodedb.temp_voucherpattern WHERE VoucherTypeID = 9 AND IsDebit=1 ;
        SELECT slid INTO VSLIDcommission0 FROM nodedb.temp_voucherpattern WHERE VoucherTypeID = 9 AND IsDebit=0 ;
        SELECT slid INTO VSLIDsd1 FROM nodedb.temp_voucherpattern WHERE VoucherTypeID = 18 AND IsDebit=1 ;
        SELECT slid INTO VSLIDsd0 FROM nodedb.temp_voucherpattern WHERE VoucherTypeID = 18 AND IsDebit=0 ;
        SELECT slid INTO VSLIDsca1 FROM nodedb.temp_voucherpattern WHERE VoucherTypeID = 10 AND IsDebit=1 ;
        SELECT slid INTO VSLIDsca0 FROM nodedb.temp_voucherpattern WHERE VoucherTypeID = 10 AND IsDebit=0 ;
        SELECT slid INTO VSLIDst1 FROM nodedb.temp_voucherpattern WHERE VoucherTypeID = 6 AND IsDebit=1 ;
        SELECT slid INTO VSLIDst0 FROM nodedb.temp_voucherpattern WHERE VoucherTypeID = 6 AND IsDebit=0 ;
        DROP TEMPORARY TABLE nodedb.temp_voucherpattern;
        
        INSERT INTO golrokh1402.voucher(description, BusinessUnitID, createdBy,month)
           VALUES (VdesVoucher,Vbusinessunitid,Vcreatby,Vmonth);
        
        SELECT id INTO Vvoucherid FROM golrokh1402.voucher WHERE createdBy=Vcreatby order by id DESC limit 1;
        SET Vdestr=VdesVoucher;
        
        UPDATE golrokh.orders SET statusID=5,VoucherID=Vvoucherid,modifiedOn=CURRENT_TIMESTAMP,modifiedBy=Vcreatby WHERE id=Vorderid ;
        
OPEN cursorSALEITEM;

loopSALEITEM: LOOP

 FETCH cursorSALEITEM INTO
  VOPid,VDLIDC,VQty,Vunitprice,Vdiscount,Vtax,Vcommission,VProductcode,
  VProductname,VStandardCost,Vunitname;

 IF done THEN
   LEAVE loopSALEITEM;
 END IF;

 SET Vtotaltax = Vtotaltax+Vtax;
 SET Vdiscount=Vdiscount/100 ;
 SET Vcost=Vunitprice*(1-Vdiscount);
 SET VAmount=VQty*Vcost;SET VAmount=ROUND(VAmount,0);
 SET VAmountLine=VAmount;
 SET VProductname=CONCAT(VProductname,' (',VProductcode,')');
 SET VdescriptionItem='';
 SET VdescriptionItem=REPLACE(VdesVoucherItem, "{1}", CONVERT(VQty, CHAR));
 SET VdescriptionItem=REPLACE(VdescriptionItem,"{2}",Vunitname);
 SET VdescriptionItem=REPLACE(VdescriptionItem,"{3}",VProductname);
 SET Vdes=VdescriptionItem;
 SET Vformatted=FORMAT(Vcost,0);
 SET Vformatted=CONCAT(Vformatted,' ریال');
 SET VdescriptionItem=CONCAT(VdescriptionItem,' ',Vformatted); 
 SET VdesComm=REPLACE(VdesVoucherItemcomm,"{2}",Vformatted);
 IF VAmount>0 THEN
   INSERT INTO golrokh1402.voucheritem
      ( voucherid, slid, DLID, Amount, description,eventdate) VALUES
      (Vvoucherid,VSLIDs1,VDLIDD,VAmount,VdescriptionItem,Veventdate),
      (Vvoucherid,VSLIDs0,VDLIDC,VAmount,VdescriptionItem,Veventdate);
  END IF;
  SELECT Qty INTO VQtyinv FROM golrokh.vw_qty WHERE ProductID=VDLIDC AND fiscalyear=Vfiscalyear ;
  IF (VQtyinv IS NOT NULL AND VQtyinv >= 1) THEN
     SELECT SUM(Amount) INTO VSUMAmount FROM golrokh1402.voucheritem
                                      WHERE DLID =VDLIDC AND slid=VSLIDcost1 ;
     SET Vcost=VSUMAmount/VQtyinv;
  ELSE
     SET Vcost=VStandardCost;
  END IF;

  SET Vcost=ROUND(Vcost,0);

  UPDATE golrokh.producttransaction SET
          type=6,
          modifiedOn= CURRENT_TIMESTAMP ,
          modifiedBy= Vcreatby,
          cost= Vcost
   WHERE id = VOPid ;

  SET Vformatted=FORMAT(Vcost,0);

  SET VdescriptionItem=CONCAT(Vdes,' ',Vformatted,' ریال');

  SET VAmount=VQty*Vcost;
  SET VAmount=ROUND(VAmount,0);

  INSERT INTO golrokh1402.voucheritem
  ( voucherid, slid, DLID, Amount, description,eventdate) VALUES
  (Vvoucherid,VSLIDcost1,VDLIDC,VAmount,VdescriptionItem,Veventdate),
  (Vvoucherid,VSLIDcost0,VDLIDC,-VAmount,VdescriptionItem,Veventdate);

  IF Vcommission>0 THEN
    SET VdescriptionItem=REPLACE(VdesComm,"{1}",Vcommission);
    SET VAmount=Vcommission*VAmountLine/100;
    SET VAmount=ROUND(VAmount,0);

    IF VAmount>0 THEN
      INSERT INTO golrokh1402.voucheritem
           ( voucherid, slid, DLID, Amount, description,eventdate) VALUES
         (Vvoucherid,VSLIDcommission1,VDLIDDcommission,VAmount,VdescriptionItem,Veventdate),
         (Vvoucherid,VSLIDcommission0,VDLIDCcommission,-VAmount,VdescriptionItem,Veventdate);
    END IF;

  END IF;

END LOOP loopSALEITEM;

CLOSE cursorSALEITEM;

IF Vtotaltax>0 THEN
SET VdesVoucher=CONCAT('بابت مالبات ارزش افزوده فاکتور فروش شماره ',VNumberSale);
SET Vtotaltax=round(Vtotaltax,0);
INSERT INTO golrokh1402.voucheritem
  ( voucherid, slid, DLID, Amount, description,eventdate) VALUES
     (Vvoucherid,VSLIDst1,VDLIDD,Vtotaltax,VdesVoucher,Veventdate),
     (Vvoucherid,VSLIDst0,VDLIDt,-Vtotaltax,VdesVoucher,Veventdate);
END IF;


IF Vinputdiscount>0 THEN
SET VdesVoucher=CONCAT('بابت تخفیف فاکتور فروش شماره ',VNumberSale);
INSERT INTO golrokh1402.voucheritem
( voucherid, slid, DLID, Amount, description,eventdate) VALUES
  (Vvoucherid,VSLIDsd1,VDLIDdis,Vinputdiscount,VdesVoucher,Veventdate),
  (Vvoucherid,VSLIDsd0,VDLIDD,-Vinputdiscount,VdesVoucher,Veventdate);
END IF;

INSERT INTO golrokh.transactions( description, DLID, Amount, type, numoftype, fiscalyear, createdBy) VALUES
  (Vdestr,VDLIDD,Vtotalor,2,Vorderid,Vfiscalyear,Vcreatby);

IF Vcashamound>0 THEN
SET VdesVoucher=CONCAT('دریافت نقدی بابت فاکتور ',VNumberSale,' از ',Vnameofcustomer);
INSERT INTO golrokh1402.voucheritem
            ( voucherid, slid, DLID, Amount, description,eventdate) VALUES
               (Vvoucherid,VSLIDsca1,Vfund,Vcashamound,VdesVoucher,Veventdate),
               (Vvoucherid,VSLIDsca0,VDLIDD,-Vcashamound,VdesVoucher,Veventdate);
INSERT INTO golrokh.transactions( description, DLID, Amount, type, numoftype, fiscalyear, createdBy)
VALUES (VdesVoucher,VDLIDD,Vcashamound,6,0,Vfiscalyear,Vcreatby);
ELSEIF Vcashamound<0 THEN
  SET VdesVoucher=CONCAT('عودت وجه بابت فاکتور ',VNumberSale,' به ',Vnameofcustomer);
  INSERT INTO golrokh1402.voucheritem
          ( voucherid, slid, DLID, Amount, description,eventdate) VALUES
             (Vvoucherid,VSLIDsca0,VDLIDD,-Vcashamound,VdesVoucher,Veventdate),
             (Vvoucherid,VSLIDsca1,Vfund,Vcashamound,VdesVoucher,Veventdate);
  INSERT INTO golrokh.transactions( description, DLID, Amount, type, numoftype, fiscalyear, createdBy)
  VALUES (VdesVoucher,VDLIDD,Vcashamound,6,0,Vfiscalyear,Vcreatby);
END IF;
COMMIT;
END//
DELIMITER;
