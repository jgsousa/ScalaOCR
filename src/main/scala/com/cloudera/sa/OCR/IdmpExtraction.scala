package com.cloudera.sa.OCR

import java.awt.Image
import java.awt.image.RenderedImage
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.nio.ByteBuffer
import java.util.List
import javax.imageio.ImageIO

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.bytedeco.javacpp.lept._
import org.bytedeco.javacpp.tesseract._
import org.ghost4j.document.PDFDocument
import org.ghost4j.renderer.SimpleRenderer

import scala.collection.JavaConversions._
import scala.collection.mutable.StringBuilder

object IdmpExtraction {
  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName("IDMP Processor")
    val sc = new SparkContext(conf)

    /** Read in PDFs into the RDD */
    val files = sc.binaryFiles ("hdfs://quickstart.cloudera/user/hive/warehouse/510k/pdfs")
    files.map(convertFunc(_)).count
  }


  /** Populate the HBase table
    *	@param fileName This corresponds to the rowID in HBase
    * 	@param lines The parsed output.
    *	dataformat: binaryPDF:PortableDataStream
    */
  def populateHbase (
                      fileName:String,
                      lines: String,
                      pdf:org.apache.spark.input.PortableDataStream) : Unit =
  {
    /** Configure and open a HBase connection */
    val conf = HBaseConfiguration.create()
    val conn=  ConnectionFactory.createConnection( conf );
    val mddsTbl = conn.getTable( TableName.valueOf( "mdds" ));
    val cf = "info"
    val put = new Put( Bytes.toBytes( fileName ))

    /**
      *	Extract Fields here using Regexes
      *	Create Put objects and send to hbase
      */
    val aAndCP = """(?s)(?m).*\d\d\d\d\d-\d\d\d\d(.*)\nRe: (\w\d\d\d\d\d\d).*""".r
    val approvedP = """(?s)(?m).*(You may, therefore, market the device, subject to the general controls provisions of the Act).*""".r

    lines match {
      case
        aAndCP( addr, casenum ) => put.add( Bytes.toBytes( cf ), Bytes.toBytes( "submitter_info" ), Bytes.toBytes( addr ) ).add( Bytes.toBytes( cf ), Bytes.toBytes( "case_num" ), Bytes.toBytes( casenum ))
      case _ => println( "did not match a regex" )
    }

    lines match {
      case
        approvedP( approved ) => put.add( Bytes.toBytes( cf ), Bytes.toBytes( "approved" ), Bytes.toBytes( "yes" ))
      case _ => put.add( Bytes.toBytes( cf ), Bytes.toBytes( "approved" ), Bytes.toBytes( "no" ))
    }

    System.out.println("Start of regex");

    lines.split("\n").foreach {

      val regNumRegex = """Regulation Number:\s+(.+)""".r
      val regNameRegex = """Regulation Name:\s+(.+)""".r
      val regClassRegex = """Regulatory Class:\s+(.+)""".r
      val productCodeRegex = """Product Code:\s+(.+)""".r
      val datedRegex = """Dated:\s+(\w{3,10}\s+\d{1,2},\s+\d{4}).*""".r
      val receivedRegex = """Received:\s+(\w{3,10}\s+\d{1,2},\s+\d{4}).*""".r
      val deviceNameRegex = """Trade/Device Name:\s+(.+)""".r

      _ match {
        case regNumRegex( regNum ) => put.add( Bytes.toBytes( cf ), Bytes.toBytes( "reg_num" ), Bytes.toBytes( regNum ))
        case regNameRegex(regName) => put.add(Bytes.toBytes( cf ), Bytes.toBytes( "reg_name" ), Bytes.toBytes( regName ))
        case regClassRegex( regClass ) => put.add( Bytes.toBytes( cf ), Bytes.toBytes( "reg_class" ), Bytes.toBytes( regClass ))
        case productCodeRegex( productCode ) => put.add( Bytes.toBytes( cf ), Bytes.toBytes( "product_code" ), Bytes.toBytes( productCode ))
        case datedRegex( dated ) => put.add( Bytes.toBytes( cf ), Bytes.toBytes( "dated" ), Bytes.toBytes( dated ))
        case receivedRegex( received ) => put.add( Bytes.toBytes( cf ), Bytes.toBytes( "received" ), Bytes.toBytes( received ))
        case deviceNameRegex( deviceName ) => put.add( Bytes.toBytes( cf ), Bytes.toBytes( "device_name" ), Bytes.toBytes( deviceName ))

        case _ => print( "" )
      }
    }

    System.out.println("End of Regex");

    put.add( Bytes.toBytes( cf ), Bytes.toBytes( "text" ), Bytes.toBytes( lines ))
    val pdfBytes = pdf.toArray.clone
    put.add(Bytes.toBytes( "obj" ), Bytes.toBytes( "pdf" ), pdfBytes )

    System.out.println("Begin put of document")
    mddsTbl.put( put )
    mddsTbl.close
    conn.close
    System.out.println("End put of document")

  }

  /** Method to convert a PDF document to images and hence OCR
    */
  def convertFunc (
                    file: (String, org.apache.spark.input.PortableDataStream)
                  ) : Unit  =
  {
    /** Render the PDF into a list of images with 300 dpi resolution
      * One image per PDF page, a PDF document may have multiple pages
      */

    System.out.println("Processing document:" + file._2.getPath())
    val document: PDFDocument = new PDFDocument( )
    document.load( file._2.open )
    file._2.close
    val renderer :SimpleRenderer = new SimpleRenderer( )
    renderer.setResolution( 300 )
    System.out.println("Converting to image document:" + file._2.getPath())
    try
      {
      val images:List[Image] = renderer.render( document )

      /**  Iterate through the image list and extract OCR
        * using Tesseract API.
        */
      var r:StringBuilder = new StringBuilder
      images.toList.foreach{ x=>
        val imageByteStream = new ByteArrayOutputStream( )
        ImageIO.write(
          x.asInstanceOf[RenderedImage], "png", imageByteStream )
        val pix: PIX = pixReadMem(
          ByteBuffer.wrap( imageByteStream.toByteArray( ) ).array( ),
          ByteBuffer.wrap( imageByteStream.toByteArray( ) ).capacity( )
        )
        val api: TessBaseAPI = new TessBaseAPI( )
        /** We assume the documents are in English here, hence \”eng\” */
        System.out.println("OCR of document:" + file._2.getPath())
        api.Init( null, "eng" )
        api.SetImage(pix)
        r.append(api.GetUTF8Text().getString())
        imageByteStream.close
        pixDestroy(pix)
        api.End
        System.out.println("End of OCR of document:" + file._2.getPath())
      }

      /** Write the generated data into HBase */
      System.out.println("Begin insert of document:" + file._2.getPath())
      populateHbase( file._1, r.toString( ), file._2 )
      System.out.println("End insert of document:" + file._2.getPath())
    }
    catch(){

    }

  }
}