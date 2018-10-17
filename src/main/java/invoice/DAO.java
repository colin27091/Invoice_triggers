package invoice;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;

public class DAO {

	private final DataSource myDataSource;

	/**
	 *
	 * @param dataSource la source de données à utiliser
	 */
	public DAO(DataSource dataSource) {
		this.myDataSource = dataSource;
	}

	/**
	 * Renvoie le chiffre d'affaire d'un client (somme du montant de ses factures)
	 *
	 * @param id la clé du client à chercher
	 * @return le chiffre d'affaire de ce client ou 0 si pas trouvé
	 * @throws SQLException
	 */
	public float totalForCustomer(int id) throws SQLException {
		String sql = "SELECT SUM(Total) AS Amount FROM Invoice WHERE CustomerID = ?";
		float result = 0;
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setInt(1, id); // On fixe le 1° paramètre de la requête
			try (ResultSet resultSet = statement.executeQuery()) {
				if (resultSet.next()) {
					result = resultSet.getFloat("Amount");
				}
			}
		}
		return result;
	}

	/**
	 * Renvoie le nom d'un client à partir de son ID
	 *
	 * @param id la clé du client à chercher
	 * @return le nom du client (LastName) ou null si pas trouvé
	 * @throws SQLException
	 */
	public String nameOfCustomer(int id) throws SQLException {
		String sql = "SELECT LastName FROM Customer WHERE ID = ?";
		String result = null;
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setInt(1, id);
			try (ResultSet resultSet = statement.executeQuery()) {
				if (resultSet.next()) {
					result = resultSet.getString("LastName");
				}
			}
		}
		return result;
	}

	/**
	 * Transaction permettant de créer une facture pour un client
	 *
	 * @param customer Le client
	 * @param productIDs tableau des numéros de produits à créer dans la facture
	 * @param quantities tableau des quantités de produits à facturer faux sinon Les deux tableaux doivent avoir la même
	 * taille
	 * @throws java.lang.Exception si la transaction a échoué
	 */
	public void createInvoice(CustomerEntity customer, int[] productIDs, int[] quantities) throws Exception {
            String it = "INSERT INTO Item VALUES(?,?,?,?,?)"; /*Initialisation des requetes SQL*/
            String inv = "INSERT INTO Invoice(CustomerID) VALUES(?)";
            String price = "SELECT Price FROM Product WHERE ID = ?";
            try (	Connection myConnection = myDataSource.getConnection(); /*Creation de la connection dans l'environnement*/
			PreparedStatement statement_inv = myConnection.prepareStatement(inv, Statement.RETURN_GENERATED_KEYS);/*On ajoute les requete à l'environnement*/
                        PreparedStatement statement_it = myConnection.prepareStatement(it);
                        PreparedStatement statement_price = myConnection.prepareStatement(price)) {
                
                        myConnection.setAutoCommit(false);/*Debut de creation de la facture*/
			try {
				statement_inv.setInt( 1, customer.getCustomerId());/*On commence par creer l'invoice avec le paramètre customerID*/
                                statement_inv.executeUpdate();/*On execute la requete*/
                                ResultSet clefs = statement_inv.getGeneratedKeys();/*On recupere l'ID de l'invoice crée automatiquement*/
                                clefs.next();
                                    if(productIDs.length != quantities.length){/*On verifie que les deux tableaux sont de longueurs égales*/
                                        throw new Exception();
                                    }
                                    for(int i = 0; i < productIDs.length; i++){/*Pour chaque produit on crée un Item*/
                                        statement_it.setInt(1, (int) clefs.getInt(1));/*Avec l'ID de l'invoice recuperer au dessus*/
                                        statement_it.setInt(2, i);/*On lui affecte un ID (l'indice du tableaux)*/
                                        statement_it.setInt(3, productIDs[i]);/*On place l'ID du produits*/
                                        statement_it.setInt(4,quantities[i]);/*On place la quantité du produit*/
                                        float pr = 0;/*On initialise pr qui sera le*/
                                        statement_price.setInt(1, productIDs[i]);/*On calcule le pr en lancant une requete SQL*/
                                        try (ResultSet resultSet = statement_price.executeQuery()) {
                                            resultSet.next();
                                            pr = resultSet.getFloat("Price");/*On recupère le prix d'un produit*/
                                            
                                        }
                                        statement_it.setFloat(5, pr);/*On rajoute pr comme le cost, il est multiplier automatiquement avec la quantite grace au triggers*/
                                        int numberUpdated = statement_it.executeUpdate();
                                        if(numberUpdated == 0 ){
                                            throw new Exception();
                                        }
                                    }
				myConnection.commit();/*On commit la transaction*/
			} catch (Exception ex) {
				myConnection.rollback();
				throw ex;
                        } finally {
				myConnection.setAutoCommit(true);/*On revient à l'état de base*/				
			}
            }
        }

	/**
	 *
	 * @return le nombre d'enregistrements dans la table CUSTOMER
	 * @throws SQLException
	 */
	public int numberOfCustomers() throws SQLException {
		int result = 0;

		String sql = "SELECT COUNT(*) AS NUMBER FROM Customer";
		try (Connection connection = myDataSource.getConnection();
			Statement stmt = connection.createStatement()) {
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next()) {
				result = rs.getInt("NUMBER");
			}
		}
		return result;
	}

	/**
	 *
	 * @param customerId la clé du client à recherche
	 * @return le nombre de bons de commande pour ce client (table PURCHASE_ORDER)
	 * @throws SQLException
	 */
	public int numberOfInvoicesForCustomer(int customerId) throws SQLException {
		int result = 0;

		String sql = "SELECT COUNT(*) AS NUMBER FROM Invoice WHERE CustomerID = ?";

		try (Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setInt(1, customerId);
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				result = rs.getInt("NUMBER");
			}
		}
		return result;
	}

	/**
	 * Trouver un Customer à partir de sa clé
	 *
	 * @param customedID la clé du CUSTOMER à rechercher
	 * @return l'enregistrement correspondant dans la table CUSTOMER, ou null si pas trouvé
	 * @throws SQLException
	 */
	CustomerEntity findCustomer(int customerID) throws SQLException {
		CustomerEntity result = null;

		String sql = "SELECT * FROM Customer WHERE ID = ?";
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setInt(1, customerID);

			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				String name = rs.getString("FirstName");
				String address = rs.getString("Street");
				result = new CustomerEntity(customerID, name, address);
			}
		}
		return result;
	}

	/**
	 * Liste des clients localisés dans un état des USA
	 *
	 * @param state l'état à rechercher (2 caractères)
	 * @return la liste des clients habitant dans cet état
	 * @throws SQLException
	 */
	List<CustomerEntity> customersInCity(String city) throws SQLException {
		List<CustomerEntity> result = new LinkedList<>();

		String sql = "SELECT * FROM Customer WHERE City = ?";
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setString(1, city);
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					int id = rs.getInt("ID");
					String name = rs.getString("FirstName");
					String address = rs.getString("Street");
					CustomerEntity c = new CustomerEntity(id, name, address);
					result.add(c);
				}
			}
		}

		return result;
	}
}
