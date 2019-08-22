package tutorial.storm.trident.example;

public class Customer {
    private Long id;
    private String nome;

    public Customer() {
    }

    public Customer(Long id, String nome) {
        this.id = id;
        this.nome = nome;
    }

    public Long getId() {
        return id;
    }

    public String getNome() {
        return nome;
    }

    @Override
    public String toString() {
        return "Customer{" +
                "id=" + id +
                ", nome='" + nome + '\'' +
                '}';
    }
}